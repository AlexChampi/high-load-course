package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.TokenBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger
import kotlin.system.measureNanoTime

// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()

        private const val THREAD_SLEEP_MILLIS = 5L
        private const val PROCESSING_TIME_MILLIS = 5000
        private const val MAX_RETRY_COUNT = 3
        private val RETRYABLE_HTTP_CODES = setOf(429, 500, 502, 503, 504)
        private const val DELAY_DURATION_MILLIS = 100L
        private const val MAX_PAYMENT_REQUEST_DURATION = 1500L
        private const val REQUEST_COUNT = 800
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
//    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val requestAverageProcessingTime = Duration.ofMillis(500)
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val map = HashMap<UUID, Int>()
    private val z = AtomicInteger(0)
    private val v = AtomicInteger(0)
    val responseTimes = ConcurrentLinkedQueue<Long>()


    private val client = OkHttpClient.Builder().callTimeout(Duration.ofMillis(1500)).build()

    //    private val client = OkHttpClient.Builder().build()
    private val rateLimiter = TokenBucketRateLimiter(
        rate = rateLimitPerSec,
        window = 1005,
        bucketMaxCapacity = rateLimitPerSec,
        timeUnit = TimeUnit.MILLISECONDS
    )

    private val semaphore = Semaphore(5, true)
    private val acquireMaxWaitMillis = PROCESSING_TIME_MILLIS - requestAverageProcessingTime.toMillis()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        v.getAndIncrement()
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }


        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            post(emptyBody)
        }.build()

        var isAcquired = false
        var threadWaitTime = 0L
        var curIteration = 0

        while (curIteration < MAX_RETRY_COUNT) {
            var isRetryableWithDelay = false

            try {
                logger.info("before try semaphore for $paymentId")
                isAcquired = semaphore.tryAcquire(acquireMaxWaitMillis - threadWaitTime, TimeUnit.MILLISECONDS)
                logger.info("after try semaphore for $paymentId")
                if (!isAcquired) {
                    throw TimeoutException("Failed to acquire permission to process payment")
                }

                logger.info("cur iteration $curIteration for $paymentId")


                logger.info("before try to tick for $paymentId")
                while (!rateLimiter.tick()) {
                    threadWaitTime += THREAD_SLEEP_MILLIS
                    Thread.sleep(THREAD_SLEEP_MILLIS)
                    logger.info("ticking for $paymentId")
                }
                logger.info("success tick for $paymentId")


                val duration = measureNanoTime {
                    try {
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
                            if (v.get() % 50 == 0) {
                                logger.info("?????????????????????????????????????????????????? " + map)
                                logger.info(" %%%%%%%%%%%%%%%%%%%%%% Response Time Percentiles: ${calculatePercentiles()}")
                            }
                            if (body.result) return
                            if (RETRYABLE_HTTP_CODES.contains(response.code)) isRetryableWithDelay = true
                        }
                    } catch (e: Exception) {
                    }
                }
                responseTimes.add(duration / 1_000_000)

                curIteration++
                map[paymentId] = curIteration
                z.getAndIncrement()
                logger.info("!!!!!!!!!!!!!!!! " + z)
//                logger.info("!!!!!!!!!!!!!!!!"+ map.size + " " + map.toString())


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

            logger.warn("[$accountName]/ Retry for payment processed for txId: $transactionId, payment: $paymentId")
            if (curIteration < MAX_RETRY_COUNT) { //&& isRetryableWithDelay) {
                Thread.sleep(DELAY_DURATION_MILLIS)
            }
//                if (now() + requestAverageProcessingTime.toMillis() >= deadline){
//                    logger.info("govno for sanya $curIteration for $paymentId")
//                    return
//                }
            logger.info("finish cur iteration $curIteration for $paymentId")
        }
//        if (now() + requestAverageProcessingTime.toMillis() >= deadline) return

        logger.info("result iteration cout $curIteration")
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    fun calculatePercentiles(): Map<Int, Long> {
        val sortedTimes = responseTimes.sorted()
        return mapOf(
            1 to percentile(sortedTimes, 1),
            2 to percentile(sortedTimes, 2),
            3 to percentile(sortedTimes, 3),
            4 to percentile(sortedTimes, 4),
            5 to percentile(sortedTimes, 5),
            6 to percentile(sortedTimes, 6),
            7 to percentile(sortedTimes, 7),
            8 to percentile(sortedTimes, 8),
            9 to percentile(sortedTimes, 9),
            10 to percentile(sortedTimes, 10),
            20 to percentile(sortedTimes, 20),
            30 to percentile(sortedTimes, 30),
            40 to percentile(sortedTimes, 40),
            41 to percentile(sortedTimes, 41),
            42 to percentile(sortedTimes, 42),
            43 to percentile(sortedTimes, 43),
            44 to percentile(sortedTimes, 44),
            45 to percentile(sortedTimes, 45),
            46 to percentile(sortedTimes, 46),
            47 to percentile(sortedTimes, 47),
            48 to percentile(sortedTimes, 48),
            49 to percentile(sortedTimes, 49),
            50 to percentile(sortedTimes, 50),
            90 to percentile(sortedTimes, 90),
            95 to percentile(sortedTimes, 95),
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