import okhttp3.*
import ru.quipy.payments.logic.MetricInterceptor.OkHttpMetricsInterceptor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class MultiClientManager {
    private val clientCount = 5
    private val clients: List<OkHttpClient>
    private val counter = AtomicInteger(0)

    init {
        clients = List(clientCount) {
            OkHttpClient.Builder()
//                .connectionPool(ConnectionPool(100, 10, TimeUnit.MINUTES))
                .dispatcher(Dispatcher().apply {
                    maxRequests = 1100
                    maxRequestsPerHost = 1100
                })
                .protocols(listOf(Protocol.HTTP_2, Protocol.HTTP_1_1))
                .addInterceptor(OkHttpMetricsInterceptor())
                .build()
        }
    }

    fun getClient(): OkHttpClient {
        val index = counter.getAndIncrement() % clientCount
        return clients[index]
    }
}
