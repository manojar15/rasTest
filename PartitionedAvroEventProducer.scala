package rastest

import java.util.Properties
import java.io.ByteArrayOutputStream
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.nio.ByteBuffer

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, DatumWriter, EncoderFactory}
import org.apache.avro.generic.GenericDatumWriter
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.SparkSession
import scala.io.Source
import scala.util.Random

object ParallelPartitionedAvroEventProducer {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Parallel Partitioned Avro Event Producer")
      .master("local[8]") // or local[*] to use all available cores
      .config("spark.executor.memory", "16g") // Half of your system RAM
      .config("spark.driver.memory", "16g")
      .config("spark.executor.heartbeatInterval", "60s")
      .config("spark.network.timeout", "300s")
      .config("spark.sql.shuffle.partitions", "64")
      .getOrCreate()

    import spark.implicits._

    val kafkaTopic = "customer_events_partitioned_v2"
    val kafkaBootstrapServers = "localhost:9092"
    val tenantId = 42
    val logicalDate = LocalDate.parse("2025-04-03", DateTimeFormatter.ISO_DATE)
    val logicalDateDays = logicalDate.toEpochDay.toInt
    val eventTypes = Seq("NAME", "ADDRESS", "IDENTIFICATION")

    // Load and broadcast Avro schemas
    val wrapperSchemaStr = Source.fromFile("src/main/avro/CustomerEvent.avsc").mkString
    val nameSchemaStr = Source.fromFile("src/main/avro/NamePayload.avsc").mkString
    val addressSchemaStr = Source.fromFile("src/main/avro/AddressPayload.avsc").mkString
    val idSchemaStr = Source.fromFile("src/main/avro/IdentificationPayload.avsc").mkString

    val wrapperSchema = new Schema.Parser().parse(wrapperSchemaStr)
    val headerSchema = wrapperSchema.getField("header").schema()
    val nameSchema = new Schema.Parser().parse(nameSchemaStr)
    val addressSchema = new Schema.Parser().parse(addressSchemaStr)
    val idSchema = new Schema.Parser().parse(idSchemaStr)

    // Load customer metadata and broadcast it
    val customerMetaPath = "C:/Users/e5655076/RAS_RPT/obrandrastest/customer/customer_metadata"
    val existingCustomerIds = spark.read.parquet(customerMetaPath)
      .select("customer_id").as[String].collect().toSet
    val broadcastCustomerIds = spark.sparkContext.broadcast(existingCustomerIds.toIndexedSeq)

    // Kafka config
    val kafkaProps = new Properties()
    kafkaProps.put("bootstrap.servers", kafkaBootstrapServers)
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    kafkaProps.put("linger.ms", "10")
    kafkaProps.put("batch.size", "65536")
    kafkaProps.put("compression.type", "snappy")

    def generateAndSendEvent(customerId: String, eventType: String, producer: KafkaProducer[String, Array[Byte]]): Unit = {
      val header = new GenericData.Record(headerSchema)
      header.put("event_timestamp", System.currentTimeMillis())
      header.put("logical_date", logicalDateDays)
      header.put("event_type", eventType)
      header.put("tenant_id", tenantId)
      header.put("entity_id", customerId)

      val (schema, payload) = eventType match {
        case "NAME" =>
          val rec = new GenericData.Record(nameSchema)
          rec.put("customer_id", customerId)
          rec.put("first", s"UpdatedFirst_$customerId")
          rec.put("middle", "Z")
          rec.put("last", s"UpdatedLast_$customerId")
          (nameSchema, rec)
        case "ADDRESS" =>
          val rec = new GenericData.Record(addressSchema)
          rec.put("customer_id", customerId)
          rec.put("type", new GenericData.EnumSymbol(addressSchema.getField("type").schema(), if (new Random().nextBoolean()) "HOME" else "WORK"))
          rec.put("street", s"NewStreet $customerId")
          rec.put("city", s"NewCity_$customerId")
          rec.put("postal_code", f"NewPC_${new Random().nextInt(999)}%03d")
          rec.put("country", "NewCountryX")
          (addressSchema, rec)
        case "IDENTIFICATION" =>
          val rec = new GenericData.Record(idSchema)
          rec.put("customer_id", customerId)
          rec.put("type", "passport")
          rec.put("number", s"NEWID_$customerId")
          rec.put("issuer", "GovX")
          (idSchema, rec)
      }

      val payloadOut = new ByteArrayOutputStream()
      val payloadEncoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(payloadOut, null)
      val payloadWriter: DatumWriter[GenericRecord] = new GenericDatumWriter[GenericRecord](schema)
      payloadWriter.write(payload, payloadEncoder)
      payloadEncoder.flush()
      payloadOut.close()

      val wrapper = new GenericData.Record(wrapperSchema)
      wrapper.put("header", header)
      wrapper.put("payload", ByteBuffer.wrap(payloadOut.toByteArray))

      val out = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get().binaryEncoder(out, null)
      val writer: DatumWriter[GenericRecord] = new GenericDatumWriter[GenericRecord](wrapperSchema)
      writer.write(wrapper, encoder)
      encoder.flush()
      out.close()

      val record = new ProducerRecord[String, Array[Byte]](kafkaTopic, customerId, out.toByteArray)
      producer.send(record)
    }

    // ----------- 4M events for existing customers ------------
    val random = new Random()
    val existingEvents = spark.sparkContext.parallelize(1 to 4000000, numSlices = 64)

    existingEvents.foreachPartition { part =>
      val rnd = new Random()
      val producer = new KafkaProducer[String, Array[Byte]](kafkaProps)
      val customers = broadcastCustomerIds.value

      part.foreach { _ =>
        val customerId = customers(rnd.nextInt(customers.size))
        val eventType = eventTypes(rnd.nextInt(eventTypes.size))
        generateAndSendEvent(customerId, eventType, producer)
      }

      producer.close()
    }

    // ----------- 500K NEW customers (5 events each) ----------
    val newCustomerRange = spark.sparkContext.parallelize(1 to 500000, numSlices = 32)

    newCustomerRange.foreachPartition { part =>
      val producer = new KafkaProducer[String, Array[Byte]](kafkaProps)
      val rnd = new Random()

      part.foreach { i =>
        val partitionId = rnd.nextInt(8)
        val newCustomerId = f"${partitionId}_9${rnd.nextInt(999999)}%06d"

        generateAndSendEvent(newCustomerId, "NAME", producer)
        generateAndSendEvent(newCustomerId, "ADDRESS", producer)
        generateAndSendEvent(newCustomerId, "ADDRESS", producer)
        generateAndSendEvent(newCustomerId, "IDENTIFICATION", producer)
        generateAndSendEvent(newCustomerId, "IDENTIFICATION", producer)
      }

      producer.close()
    }

    println("Finished generating and sending events.")
    spark.stop()
  }
}
