package ru.quipy.payments.logic

import MultiClientManager
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.*
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.TokenBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.logic.MetricInterceptor.OkHttpMetricsInterceptor
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val instances: Int
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()

        private const val THREAD_SLEEP_MILLIS = 5L
        private const val PROCESSING_TIME_MILLIS = 6000
        private const val MAX_RETRY_COUNT = 4
        private const val MAX_PAYMENT_REQUEST_DURATION = 1500L
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val requestTimeout = requestAverageProcessingTime.multipliedBy(2)
    private val rateLimitPerSec = properties.rateLimitPerSec / instances
    private val parallelRequests = properties.parallelRequests / instances

    val responseTimes = ConcurrentLinkedQueue<Long>()

    val dispatcher = Dispatcher().apply {
        maxRequests = rateLimitPerSec
        maxRequestsPerHost = rateLimitPerSec
    }
    private val client = OkHttpClient.Builder()
        .dispatcher(dispatcher)
        .addInterceptor(OkHttpMetricsInterceptor())
        .protocols(listOf(Protocol.H2_PRIOR_KNOWLEDGE))
        .callTimeout(requestTimeout)
        .build()

    private val pool = Executors.newFixedThreadPool(rateLimitPerSec)
    val multiClientManager = MultiClientManager()
    private val rateLimiter = TokenBucketRateLimiter(
        rate = rateLimitPerSec,
        window = 1005,
        bucketMaxCapacity = rateLimitPerSec,
        timeUnit = TimeUnit.MILLISECONDS
    )

    private val semaphore = Semaphore(parallelRequests, true)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        pool.submit {
            var curIteration = 0
            while (curIteration < MAX_RETRY_COUNT) {
                val shouldRetry = performPayment(paymentId, transactionId, amount, deadline)
                if (!shouldRetry) {
                    return@submit
                }

                curIteration++
            }
        }
    }

    private fun performPayment(
        paymentId: UUID,
        transactionId: UUID,
        amount: Int,
        deadline: Long
    ): Boolean {
        val request = Request.Builder().run {
            url("http://host.docker.internal:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount&timeout=${requestTimeout}")
            post(emptyBody)
        }.build()

        while (!rateLimiter.tick()) {
            Thread.sleep(THREAD_SLEEP_MILLIS)
        }

        var isAcquired = false
        try {
            isAcquired = semaphore.tryAcquire(deadline - now(), TimeUnit.MILLISECONDS)
            if (!isAcquired) {
                logger.error(
                    "[$accountName] Payment failed to acquire semaphore for txId: $transactionId, payment: $paymentId"
                )
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Failed to acquire semaphore.")
                }
                return false
            }

            client.newCall(request).execute().use { response ->
                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)

                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }
                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
                println("DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD PROTOCOLITO. "  + response.protocol)

                // percentileTracker.recordDuration(now() - requestStarted)
                return !body.result
            }
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
        } finally {
            if (isAcquired) {
                semaphore.release()
            }
        }

        return true
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    fun calculatePercentiles(): Map<Int, Long> {
        val sortedTimes = responseTimes.sorted()
        return mapOf(
            50 to percentile(sortedTimes, 50),
            85 to percentile(sortedTimes, 85),
            86 to percentile(sortedTimes, 86),
            87 to percentile(sortedTimes, 87),
            88 to percentile(sortedTimes, 88),
            89 to percentile(sortedTimes, 89),
            90 to percentile(sortedTimes, 80),
            91 to percentile(sortedTimes, 90),
            92 to percentile(sortedTimes, 92),
            93 to percentile(sortedTimes, 93),
            94 to percentile(sortedTimes, 94),
            95 to percentile(sortedTimes, 95),
            96 to percentile(sortedTimes, 96),
            97 to percentile(sortedTimes, 97),
            98 to percentile(sortedTimes, 98),
            99 to percentile(sortedTimes, 99)
        )
    }

    fun percentile(data: List<Long>, percentile: Int): Long {
        if (data.isEmpty()) return 0
        val index = (percentile / 100.0 * data.size).toInt().coerceAtMost(data.size - 1)
        return data[index]
    }

}

public fun now() = System.currentTimeMillis()