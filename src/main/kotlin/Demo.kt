import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.time.Duration
import java.util.*

const val bootstrapServer = "127.0.0.1:9092"
const val groupId = "testGroup"
const val topic = "userInput"

class Consumer {

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            val properties = Properties().apply {
                setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
                setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
                setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
                setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
                setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            }
            val consumer = KafkaConsumer<String, String>(properties)
            consumer.subscribe(listOf(topic))
            while (true) {
                consumer.poll(Duration.ofMillis(100)).forEach {
                    println("Key: ${it.key()}; Value - ${it.value()}")
                    println("Partition: ${it.partition()}; Offset - ${it.offset()}")
                }
            }
        }
    }

}

class Producer {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            val properties = Properties().apply {
                setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
                setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
                setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            }
            val producer = KafkaProducer<String, String>(properties)

            while (true) {
                val text = readLine()
                val record = ProducerRecord<String, String>(topic, text)
                producer.send(record, { metadata, _ -> println("sent $text on ${metadata.topic()} topic") })
                producer.flush()
                producer.close()
            }
        }
    }

}

