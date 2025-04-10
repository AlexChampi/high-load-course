package ru.quipy.config;

//import org.eclipse.jetty.http2.server.HTTP2CServerConnectionFactory
//import org.springframework.boot.web.embedded.jetty.JettyServerCustomizer;
//import org.springframework.boot.web.embedded.jetty.JettyServletWebServerFactory;
import org.apache.coyote.http2.Http2Protocol
import org.springframework.boot.web.embedded.tomcat.TomcatConnectorCustomizer
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration
import ru.quipy.payments.logic.PaymentExternalSystemAdapterImpl.Companion.logger

@Configuration
class JettyCustomizer {

    @Bean
    fun tomcatConnectorCustomizer(): TomcatConnectorCustomizer {
        return TomcatConnectorCustomizer {
            try {
                (it.protocolHandler.findUpgradeProtocols().get(0) as Http2Protocol)
                    .maxConcurrentStreams = 10_000_000
            } catch (e: Exception) {
                logger.error("!!! Failed to increase number of http2 streams per connection !!!")
            }
        }
    }
}
