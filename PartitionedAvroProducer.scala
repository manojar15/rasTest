package rastest

import java.io.{ByteArrayOutputStream, File}
import java.nio.ByteBuffer
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.util.Random
import scala.io.Source

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord, GenericDatumWriter}
import org.apache.avro.file.DataFileWriter
import org.apache.avro.io.{DatumWriter, EncoderFactory}

import org.apache.spark.sql.SparkSession

object PartitionedAvroProducer {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Partitioned Avro Producer")
      .master("local[8]")
      .config("spark.executor.memory", "16g")
      .config("spark.driver.memory", "16g")
      .getOrCreate()

    val tenantId = 42
    val logicalDate = LocalDate.parse("2025-04-03", DateTimeFormatter.ISO_DATE).toEpochDay.toInt
    val outputBasePath = "C:/Users/e5655076/RAS_RPT/obrandrastest/customer/avro_output_final"

    val eventTypes = Seq("NAME", "ADDRESS", "IDENTIFICATION")

    // Load Avro schemas
    val wrapperSchemaStr = Source.fromFile("src/main/avro/CustomerEvent.avsc").mkString
    val nameSchemaStr = Source.fromFile("src/main/avro/NamePayload.avsc").mkString
    val addressSchemaStr = Source.fromFile("src/main/avro/AddressPayload.avsc").mkString
    val idSchemaStr = Source.fromFile("src/main/avro/IdentificationPayload.avsc").mkString

    val wrapperSchema = new Schema.Parser().parse(wrapperSchemaStr)
    val headerSchema = wrapperSchema.getField("header").schema()
    val nameSchema = new Schema.Parser().parse(nameSchemaStr)
    val addressSchema = new Schema.Parser().parse(addressSchemaStr)
    val idSchema = new Schema.Parser().parse(idSchemaStr)

    def generateRecord(customerId: String, eventType: String): GenericRecord = {
      val header = new GenericData.Record(headerSchema)
      header.put("event_timestamp", System.currentTimeMillis())
      header.put("logical_date", logicalDate)
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
          val addrType = if (Random.nextBoolean()) "HOME" else "WORK"
          rec.put("customer_id", customerId)
          rec.put("type", new GenericData.EnumSymbol(addressSchema.getField("type").schema(), addrType))
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
      val payloadEncoder = EncoderFactory.get().binaryEncoder(payloadOut, null)
      val payloadWriter: DatumWriter[GenericRecord] = new GenericDatumWriter
      payloadWriter.write(payload, payloadEncoder)
      payloadEncoder.flush()
      payloadOut.close()

      val wrapper = new GenericData.Record(wrapperSchema)
      wrapper.put("header", header)
      wrapper.put("payload", ByteBuffer.wrap(payloadOut.toByteArray))

      wrapper
    }

    def writeAvro(records: Seq[GenericRecord], path: String): Unit = {
      val file = new File(path)
      if (file.exists()) file.delete()
      val datumWriter: DatumWriter[GenericRecord] = new GenericDatumWriter
      val dataFileWriter = new GenericDatumWriter
      dataFileWriter.create(wrapperSchema, file)
      records.foreach(record => dataFileWriter.append(record))
      dataFileWriter.close()
    }

    // Load existing customer IDs
    val customerMetaPath = "C:/Users/e5655076/RAS_RPT/obrandrastest/customer/customer_metadata"
    val existingCustomerIds = spark.read.parquet(customerMetaPath)
      .select("customer_id").as[String].collect().toIndexedSeq
    val broadcastCustomers = spark.sparkContext.broadcast(existingCustomerIds)

    // Generate existing customer events
    val existingRecords = (1 to 10000).map { _ =>
      val customerId = broadcastCustomers.value(Random.nextInt(broadcastCustomers.value.size))
      val eventType = eventTypes(Random.nextInt(eventTypes.size))
      generateRecord(customerId, eventType)
    }

    writeAvro(existingRecords, s"$outputBasePath/existing_customers.avro")

    // Generate new customer events
    val newRecords = (1 to 1000).flatMap { _ =>
      val partitionId = Random.nextInt(8)
      val newCustomerId = f"${partitionId}_9${Random.nextInt(999999)}%06d"
      Seq(
        generateRecord(newCustomerId, "NAME"),
        generateRecord(newCustomerId, "ADDRESS"),
        generateRecord(newCustomerId, "ADDRESS"),
        generateRecord(newCustomerId, "IDENTIFICATION"),
        generateRecord(newCustomerId, "IDENTIFICATION")
      )
    }

    writeAvro(newRecords, s"$outputBasePath/new_customers.avro")

    println("✅ Avro files written using original schemas.")
    spark.stop()
  }
}
