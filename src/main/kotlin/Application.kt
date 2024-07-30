package org.larserik.examples

import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.cio.*
import io.ktor.server.engine.*
import io.ktor.server.http.content.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.server.sse.*
import io.ktor.sse.*
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.time.Duration
import java.time.Instant
import java.util.*

fun main() {
    embeddedServer(CIO, port = 8080, host = "0.0.0.0", module = Application::configureRouting)
        .start(wait = true)
}

fun Application.configureRouting() {
    install(SSE)
    val producer = KafkaProducer(KafkaConfig.producerConfig(), StringSerializer(), StringSerializer())
    val validSessions = mutableListOf<Pair<UUID, Instant>>()

    GlobalScope.launch {
        while (true) {
            validSessions.removeIf { it.second.isBefore( Instant.now().minusSeconds(60))}
            delay(5000)
        }
    }

    routing {
        get("/position") {
            producer.send(ProducerRecord(KafkaConfig.topic, """
                {"x":${requreIntegerPrameter("x")},
                "y":${requreIntegerPrameter("y")}}
                """.trimIndent()))
            call.respond(HttpStatusCode.NoContent)
        }

        get("/ping") {
            log.info("Renewing session ${call.request.queryParameters["session"]}")
            validSessions.add(UUID.fromString(call.request.queryParameters["session"]!!) to Instant.now())
            call.respond(HttpStatusCode.NoContent)
        }

        staticResources("/", "static")

        sse("/events") {
            val session = UUID.randomUUID()
            validSessions += session to Instant.now()

            send(ServerSentEvent(session.toString(), "session_started"))

            log.info("Starting a kafka consumer $session")
            val consumer = KafkaConsumer(KafkaConfig.consumerConfig("test"), StringDeserializer(), StringDeserializer())

            consumer.assign(consumer.partitionsFor(KafkaConfig.topic).map { TopicPartition(KafkaConfig.topic, it.partition()) })

            if(call.request.queryParameters["tail"] != null){
                consumer.endOffsets(consumer.assignment()).forEach { consumer.seek(it.key, it.value - call.request.queryParameters["tail"]!!.toInt()) }
            } else {
                consumer.seekToEnd(consumer.assignment())
            }

            while(validSessions.any { it.first == session}){
                consumer
                    .poll(Duration.ofSeconds(2))
                    .map (ConsumerRecord<String, String>::value)
                    .map ( String::encodeBase64 )
                    .map { ServerSentEvent(it, "position_changed") }
                    .forEach { send(it) }
            }
            send(ServerSentEvent(session.toString(), "session_ended"))
            log.info("Stopping a kafka consumer $session")
            consumer.close()
        }
    }

}

fun String.encodeBase64() = Base64.getEncoder().encodeToString(this.encodeToByteArray())

fun RoutingContext.requreIntegerPrameter(s: String) =
    call.request.queryParameters[s]
        ?.toIntOrNull()
        .let (::requireNotNull)

object KafkaConfig {
    val topic: String = "test001"
    fun producerConfig() = mapOf(
        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG to "127.0.0.1:9092"
    )

    fun consumerConfig(group: String) = mapOf(
        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG to "127.0.0.1:9092",
        ConsumerConfig.GROUP_ID_CONFIG to group,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "latest",
    )
}