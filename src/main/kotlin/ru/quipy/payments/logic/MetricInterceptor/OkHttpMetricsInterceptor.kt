package ru.quipy.payments.logic.MetricInterceptor

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Timer
import io.micrometer.prometheus.PrometheusMeterRegistry
import okhttp3.Interceptor
import okhttp3.Response
import java.io.IOException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class OkHttpMetricsInterceptor() : Interceptor {
    companion object {
        val promRegistry =
            (Metrics.globalRegistry.registries.first() as PrometheusMeterRegistry).prometheusRegistry
        private val timer: Timer = Timer.builder("http.client.requests")
            .description("Время выполнения HTTP-запросов через OkHttpClient")
            .tag("client", "okhttp")
            .publishPercentiles(0.5, 0.9, 0.99) // Перцентили
//            .publishHistogram() // Включаем гистограмму
            .register(Metrics.globalRegistry) // Регистрируем в глобальном реестре
        private val activeRequests = AtomicInteger(0)

        init {
            Gauge.builder("http.client.active_requests", activeRequests, { activeRequests -> activeRequests.get().toDouble()}  ) // Название метрики
                .description("Текущее количество запросов в обработке")
                .tag("client", "okhttp")
                .register(Metrics.globalRegistry) // Регистрируем в Prometheus
        }
    }
    override fun intercept(chain: Interceptor.Chain): Response {
        val request = chain.request()
        activeRequests.incrementAndGet()
        val startTime = System.nanoTime()
        return try {
            val response = chain.proceed(request)
            timer.record(System.nanoTime() - startTime, TimeUnit.NANOSECONDS)
            response
        } catch (e: IOException) {
            timer.record(System.nanoTime() - startTime, TimeUnit.NANOSECONDS)
            throw e
        } finally {
            activeRequests.decrementAndGet()
        }
    }
}