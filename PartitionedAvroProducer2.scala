package rastest

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, DatumWriter, EncoderFactory}
import org.apache.avro.generic.GenericDatumWriter

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{BinaryType, StringType, StructField, StructType}

import scala.io.Source
import scala.util.Random

object FileBasedPartitionedAvroEventProducer {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("File Based Partitioned Avro Event Producer")
      .master("local[*]")
      .config("spark.executor.memory", "16g")
      .config("spark.driver.memory", "16g")
      .getOrCreate()

    import spark.implicits._

    val tenantId = 42
    val logicalDate = LocalDate.parse("2025-04-03", DateTimeFormatter.ISO_DATE)
    val logicalDateDays = logicalDate.toEpochDay.toInt
    val eventTypes = Seq("NAME", "ADDRESS", "IDENTIFICATION")

    // Load Avro Schemas
    val wrapperSchemaStr = Source.fromFile("src/main/avro/CustomerEvent.avsc").mkString
    val nameSchemaStr = Source.fromFile("src/main/avro/NamePayload.avsc").mkString
    val addressSchemaStr = Source.fromFile("src/main/avro/AddressPayload.avsc").mkString
    val idSchemaStr = Source.fromFile("src/main/avro/IdentificationPayload.avsc").mkString

    val wrapperSchema = new Schema.Parser().parse(wrapperSchemaStr)
    val headerSchema = wrapperSchema.getField("header").schema()
    val nameSchema = new Schema.Parser().parse(nameSchemaStr)
    val addressSchema = new Schema.Parser().parse(addressSchemaStr)
    val idSchema = new Schema.Parser().parse(idSchemaStr)

    val customerMetaPath = "C:/Users/e5655076/RAS_RPT/obrandrastest/customer/customer_metadata"
    val outputPath = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/avro_events"

    val existingCustomerIds = spark.read.parquet(customerMetaPath)
      .select("customer_id").as[String].collect().toIndexedSeq

    val broadcastCustomers = spark.sparkContext.broadcast(existingCustomerIds)

    val eventSchema = StructType(Seq(
      StructField("key", StringType),
      StructField("value", BinaryType)
    ))

    def generateEvent(customerId: String, eventType: String): (String, Array[Byte]) = {
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
          rec.put("type", new GenericData.EnumSymbol(addressSchema.getField("type").schema(), if (Random.nextBoolean()) "HOME" else "WORK"))
          rec.put("street", s"NewStreet $customerId")
          rec.put("city", s"NewCity_$customerId")
          rec.put("postal_code", f"NewPC_${Random.nextInt(999)}%03d")
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

      (customerId, out.toByteArray)
    }

    // --- Existing customers ---
    val existingEvents = spark.sparkContext.parallelize(1 to 4000000, numSlices = 64).mapPartitions { part =>
      val customers = broadcastCustomers.value
      val rand = new Random()
      part.map { _ =>
        val customerId = customers(rand.nextInt(customers.size))
        val eventType = eventTypes(rand.nextInt(eventTypes.size))
        val partitionId = rand.nextInt(8)
        val (key, bytes) = generateEvent(customerId, eventType)
        (partitionId, (key, bytes))
      }
    }

    existingEvents
      .map { case (partitionId, (key, value)) => Row(key, value, tenantId, partitionId, logicalDate.toString) }
      .toDF("key", "value", "tenant_id", "partition_id", "logical_date")
      .write
      .partitionBy("tenant_id", "partition_id", "logical_date")
      .format("avro")
      .mode("overwrite")
      .save(outputPath)

    // --- New customers ---
    val newEvents = spark.sparkContext.parallelize(1 to 500000, numSlices = 32).flatMap { i =>
      val rand = new Random()
      val partitionId = rand.nextInt(8)
      val newCustomerId = f"${partitionId}_9${rand.nextInt(999999)}%06d"
      val events = Seq("NAME", "ADDRESS", "ADDRESS", "IDENTIFICATION", "IDENTIFICATION")
      events.map { et =>
        val (key, value) = generateEvent(newCustomerId, et)
        Row(key, value, tenantId, partitionId, logicalDate.toString)
      }
    }

    spark.createDataFrame(newEvents, StructType(Seq(
      StructField("key", StringType),
      StructField("value", BinaryType),
      StructField("tenant_id", StringType),
      StructField("partition_id", StringType),
      StructField("logical_date", StringType)
    )))
      .write
      .partitionBy("tenant_id", "partition_id", "logical_date")
      .format("avro")
      .mode("append")
      .save(outputPath)

    println("Finished writing Avro events to files.")
    spark.stop()
  }
}
