package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.*
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.TokenBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.logic.MetricInterceptor.OkHttpMetricsInterceptor
import java.net.SocketTimeoutException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.http.HttpTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.*

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
        private const val MAX_RETRY_COUNT = 0
        private const val MAX_PAYMENT_REQUEST_DURATION = 1500L
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val requestTimeout = requestAverageProcessingTime.multipliedBy(2)
//    private val requestTimeout = requestAverageProcessingTime.multipliedBy(12).dividedBy(10)
//    private val rateLimitPerSec = properties.rateLimitPerSec
    private val rateLimitPerSec = 1000
    private val parallelRequests = properties.parallelRequests

    val responseTimes = ConcurrentLinkedQueue<Long>()

    private val client = HttpClient.newBuilder()
        .connectTimeout(requestTimeout)
        .version(HttpClient.Version.HTTP_2)
        .build()

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

        tryPerformPayment(paymentId, transactionId, amount, deadline, 0)
    }

    private fun tryPerformPayment(
        paymentId: UUID,
        transactionId: UUID,
        amount: Int,
        deadline: Long,
        attempt: Int
    ) {
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
                return
            }
//
//            if (!rateLimiter.tick() || !semaphore.tryAcquire()) {
//                CompletableFuture.delayedExecutor(1, TimeUnit.MILLISECONDS).execute {
//                    tryPerformPayment(paymentId, transactionId, amount, deadline, attempt)
//                }
//                return
//            }

            val uri = URI.create(
                "http://host.docker.internal:1234/external/process?serviceName=$serviceName&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount&timeout=$requestTimeout"
            )

            val request = HttpRequest.newBuilder()
                .uri(uri)
                .timeout(requestTimeout)
                .POST(HttpRequest.BodyPublishers.noBody())
                .build()

            client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .whenComplete { response, ex ->
                    semaphore.release()

                    if (ex != null) {
                        val reason = when (ex) {
                            is HttpTimeoutException -> "Request timeout"
                            else -> ex.message ?: "Unknown error"
                        }

                        logger.error("[$accountName] Payment failed: $reason", ex)
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason)
                        }

                        if (attempt < MAX_RETRY_COUNT) {
                            tryPerformPayment(paymentId, transactionId, amount, deadline, attempt + 1)
                        }

                        return@whenComplete
                    }

                    val responseBody = response.body()
                    val body = try {
                        mapper.readValue(responseBody, ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] Failed to parse response: $responseBody", e)
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, "Parse error")
                    }

                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, body.message)
                    }

                    if (!body.result && attempt < MAX_RETRY_COUNT) {
                        tryPerformPayment(paymentId, transactionId, amount, deadline, attempt + 1)
                    }
                }
        } finally {
            if (isAcquired) {
                semaphore.release()
            }
        }
    }


    /*    private fun performPayment(
            paymentId: UUID,
            transactionId: UUID,
            amount: Int,
            deadline: Long
        ): Boolean {
    //        val request = Request.Builder().run {
    //            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount&timeout=${requestTimeout}")
    //            post(emptyBody)
    //        }.build()

            val uri = URI.create(
                "http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount&timeout=${requestTimeout}"
            )

            val request = HttpRequest.newBuilder()
                .uri(uri)
                .timeout(requestTimeout)
                .POST(HttpRequest.BodyPublishers.noBody())
                .build()

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

    //            client.newCall(request).execute().use { response ->
    //                val body = try {
    //                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
    //                } catch (e: Exception) {
    //                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
    //                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
    //                }
    //                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
    //
    //                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
    //                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
    //                paymentESService.update(paymentId) {
    //                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
    //                }
    //                // percentileTracker.recordDuration(now() - requestStarted)
    //                return !body.result
    //            }


                client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).whenComplete { response, ex ->


                    if (ex != null) {
                        val reason = when (ex) {
                            is HttpTimeoutException -> "Request timeout"
                            else -> ex.message ?: "Unknown error"
                        }
                        logger.error("[$accountName] Payment failed for txId: $transactionId", ex)
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = reason)
                        }
                        return@whenComplete
                    }


                    val responseBody = response.body()


                    val body = try {
                        mapper.readValue(responseBody, ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] JSON parse error: $responseBody", e)
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, "Invalid response")
                    }
                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

    //                val body = try {
    //                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
    //                } catch (e: Exception) {
    //                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
    //                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
    //                }

                    // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                    // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
    //                 percentileTracker.recordDuration(now() - requestStarted)
    //                return !body.result
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
        }*/

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