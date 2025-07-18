package rastest

import java.io.{ByteArrayOutputStream, File}
import java.nio.ByteBuffer
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Random

import org.apache.avro._
import org.apache.avro.generic._
import org.apache.avro.file.DataFileWriter
import org.apache.avro.io.{EncoderFactory, DatumWriter}

import org.apache.spark.sql.SparkSession
import scala.io.Source

object ParallelPartitionedAvroProducer {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Avro Event File Generator")
      .master("local[8]")
      .config("spark.executor.memory", "16g")
      .config("spark.driver.memory", "16g")
      .getOrCreate()

    import spark.implicits._

    val tenantId = 42
    val logicalDate = LocalDate.parse("2025-04-03", DateTimeFormatter.ISO_DATE)
    val logicalDateDays = logicalDate.toEpochDay.toInt
    val eventTypes = Seq("NAME", "ADDRESS", "IDENTIFICATION")

    // Load schemas
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
    val avro_output = "C:/Users/e5655076/RAS_RPT/obrandrastest/customer/avro_output"
    val existingCustomerIds = spark.read.parquet(customerMetaPath)
      .select("customer_id").as[String].collect().toSet
    val broadcastCustomerIds = spark.sparkContext.broadcast(existingCustomerIds.toIndexedSeq)

    def generateEvent(customerId: String, eventType: String): GenericRecord = {
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
      val payloadEncoder = EncoderFactory.get().binaryEncoder(payloadOut, null)
      val payloadWriter = new GenericDatumWriter[GenericRecord](schema)
      payloadWriter.write(payload, payloadEncoder)
      payloadEncoder.flush()
      payloadOut.close()

      val wrapper = new GenericData.Record(wrapperSchema)
      wrapper.put("header", header)
      wrapper.put("payload", ByteBuffer.wrap(payloadOut.toByteArray))

      wrapper
    }

    // ------------ EXISTING CUSTOMERS ------------
    val existingEvents = spark.sparkContext.parallelize(1 to 16000, numSlices = 64)

    existingEvents.foreachPartition { (partIter: Iterator[Int]) =>
      val rnd = new Random()
      val customers = broadcastCustomerIds.value
      val records = partIter.map { _ =>
        val customerId = customers(rnd.nextInt(customers.size))
        val eventType = eventTypes(rnd.nextInt(eventTypes.size))
        generateEvent(customerId, eventType)
      }.toSeq

      val outFile = File.createTempFile("existing_", ".avro", new File(f"{avro_output}/existing"))
      val writer: DatumWriter[GenericRecord] = new GenericDatumWriter[GenericRecord](wrapperSchema)
      val dataFileWriter = new DataFileWriter[GenericRecord](writer)
      dataFileWriter.create(wrapperSchema, outFile)
      records.foreach(dataFileWriter.append)
      dataFileWriter.close()
    }

    // ------------ NEW CUSTOMERS ------------
    val newCustomerRange = spark.sparkContext.parallelize(1 to 4000, numSlices = 32)

    newCustomerRange.foreachPartition { (partIter: Iterator[Int]) =>
      val rnd = new Random()
      val records = partIter.flatMap { _ =>
        val partitionId = rnd.nextInt(8)
        val newCustomerId = f"${partitionId}_9${rnd.nextInt(999999)}%06d"
        Seq(
          generateEvent(newCustomerId, "NAME"),
          generateEvent(newCustomerId, "ADDRESS"),
          generateEvent(newCustomerId, "ADDRESS"),
          generateEvent(newCustomerId, "IDENTIFICATION"),
          generateEvent(newCustomerId, "IDENTIFICATION")
        )
      }.toSeq

      val outFile = File.createTempFile("new_", ".avro", new File(f"{avro_output}/new"))
      val writer: DatumWriter[GenericRecord] = new GenericDatumWriter[GenericRecord](wrapperSchema)
      val dataFileWriter = new DataFileWriter[GenericRecord](writer)
      dataFileWriter.create(wrapperSchema, outFile)
      records.foreach(dataFileWriter.append)
      dataFileWriter.close()
    }

    println("✅ Avro events generated and saved to local output directory.")
    spark.stop()
  }
}
