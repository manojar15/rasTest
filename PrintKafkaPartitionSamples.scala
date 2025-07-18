package rastest

import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerConfig, ConsumerRecords}
import org.apache.kafka.common.TopicPartition
import java.util.{Collections, Properties}
import scala.jdk.CollectionConverters._
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.apache.avro.Schema
import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import scala.io.Source

object PrintKafkaPartitionSamples {
  def main(args: Array[String]): Unit = {
    val topic = "customer_events_partitioned_v2"
    val bootstrapServers = "localhost:9092"

    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "partition-sampler-group")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

    val consumer = new KafkaConsumer[String, Array[Byte]](props)

    val wrapperSchemaJson = Source.fromFile("src/main/avro/CustomerEvent.avsc").mkString
    val namePayloadSchemaJson = Source.fromFile("src/main/avro/NamePayload.avsc").mkString
    val addressPayloadSchemaJson = Source.fromFile("src/main/avro/AddressPayload.avsc").mkString
    val idPayloadSchemaJson = Source.fromFile("src/main/avro/IdentificationPayload.avsc").mkString

    val wrapperSchema = new Schema.Parser().parse(wrapperSchemaJson)
    val nameSchema = new Schema.Parser().parse(namePayloadSchemaJson)
    val addressSchema = new Schema.Parser().parse(addressPayloadSchemaJson)
    val idSchema = new Schema.Parser().parse(idPayloadSchemaJson)

    val partitions = consumer.partitionsFor(topic).asScala.map { info =>
      new TopicPartition(topic, info.partition())
    }

    consumer.assign(partitions.asJava)
    partitions.foreach(tp => consumer.seek(tp, 0))

    println(s"Reading first decoded event from each partition of topic '$topic'\n")

    val seenPartitions = scala.collection.mutable.Set[Int]()
    val timeout = 5000 // ms

    var done = false
    while (!done) {
      val records: ConsumerRecords[String, Array[Byte]] = consumer.poll(java.time.Duration.ofMillis(timeout))
      val grouped = records.records(topic).asScala.groupBy(_.partition())
      grouped.foreach { case (partition, recs) =>
        if (!seenPartitions.contains(partition) && recs.nonEmpty) {
          val record = recs.head
          val decodedEvent = decodeAvro(record.value(), wrapperSchema)
          val header = decodedEvent.get("header").asInstanceOf[GenericRecord]
          val payloadBytes = decodedEvent.get("payload").asInstanceOf[java.nio.ByteBuffer]

          val eventType = header.get("event_type").toString
          val payloadSchema = eventType match {
            case "NAME" => nameSchema
            case "ADDRESS" => addressSchema
            case "IDENTIFICATION" => idSchema
            case _ => null
          }

          val payloadDecoded =
            if (payloadSchema != null)
              decodeAvro(payloadBytes.array(), payloadSchema).toString
            else "[unknown payload type]"

          println(s"Partition $partition | Key: ${record.key()} | EventType: $eventType | Header: $header")
          println(s"Payload: $payloadDecoded\n")
          seenPartitions += partition
        }
      }
      done = seenPartitions.size >= partitions.size
    }

    consumer.close()
  }

  def decodeAvro(bytes: Array[Byte], schema: Schema): GenericRecord = {
    val reader = new GenericDatumReader[GenericRecord](schema)
    val decoder: BinaryDecoder = DecoderFactory.get().binaryDecoder(bytes, null)
    reader.read(null, decoder)
  }
}