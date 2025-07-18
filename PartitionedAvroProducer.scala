package rastest

import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.spark.sql.SparkSession

import java.io.File
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.io.Source
import scala.util.Random

object FileBasedAvroEventProducer {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("File Based Avro Event Producer")
      .master("local[*]")
      .getOrCreate()

    val logicalDate = LocalDate.parse("2025-04-03", DateTimeFormatter.ISO_DATE)
    val logicalDateStr = logicalDate.toString
    val logicalDateDays = logicalDate.toEpochDay.toInt
    val tenantId = 42
    val eventTypes = Seq("NAME", "ADDRESS", "IDENTIFICATION")
    val rnd = new Random()

    // Load schemas
    val wrapperSchema = new Schema.Parser().parse(Source.fromFile("src/main/avro/CustomerEvent.avsc").mkString)
    val headerSchema = wrapperSchema.getField("header").schema()
    val nameSchema = new Schema.Parser().parse(Source.fromFile("src/main/avro/NamePayload.avsc").mkString)
    val addressSchema = new Schema.Parser().parse(Source.fromFile("src/main/avro/AddressPayload.avsc").mkString)
    val idSchema = new Schema.Parser().parse(Source.fromFile("src/main/avro/IdentificationPayload.avsc").mkString)

    // Load existing customer IDs
    val existingCustomerIds = spark.read
      .parquet("C:/Users/e5655076/RAS_RPT/obrandrastest/customer/customer_metadata")
      .select("customer_id")
      .as[String](spark.implicits.newStringEncoder)
      .collect()
      .toIndexedSeq
    val broadcastCustomerIds = spark.sparkContext.broadcast(existingCustomerIds)

    val baseOutputPath = s"C:/Users/e5655076/RAS_RPT/obrandrastest/customer/avro_output/$logicalDateStr"
    val existingPath = s"$baseOutputPath/existing"
    val newPath = s"$baseOutputPath/new"

    def generateWrapperRecord(customerId: String, eventType: String): GenericRecord = {
      val header = new GenericData.Record(headerSchema)
      header.put("event_timestamp", System.currentTimeMillis())
      header.put("logical_date", logicalDateDays)
      header.put("event_type", eventType)
      header.put("tenant_id", tenantId)
      header.put("entity_id", customerId)

      val (payloadSchema, payloadRecord) = eventType match {
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
          rec.put("type", new GenericData.EnumSymbol(addressSchema.getField("type").schema(), if (rnd.nextBoolean()) "HOME" else "WORK"))
          rec.put("street", s"NewStreet_$customerId")
          rec.put("city", s"NewCity_$customerId")
          rec.put("postal_code", f"NewPC_${rnd.nextInt(999)}%03d")
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

      val wrapper = new GenericData.Record(wrapperSchema)
      wrapper.put("header", header)
      wrapper.put("payload", payloadRecord) // Directly put GenericRecord (not ByteBuffer)

      wrapper
    }

    def writeAvroRecords(outputDir: String, partitionId: Int, records: Seq[GenericRecord], schema: Schema): Unit = {
      val outFile = new File(s"$outputDir/events_partition_$partitionId.avro")
      outFile.getParentFile.mkdirs()

      val writer = new GenericDatumWriter[GenericRecord](schema)
      val dataFileWriter = new DataFileWriter[GenericRecord](writer)
      dataFileWriter.create(schema, outFile)

      records.foreach(dataFileWriter.append)
      dataFileWriter.close()
    }

    // Existing customers (4M)
    val existingEvents = spark.sparkContext.parallelize(1 to 16000, numSlices = 64)
    existingEvents.mapPartitionsWithIndex { case (partitionId, part) =>
      val ids = broadcastCustomerIds.value
      val buffer = part.map { _ =>
        val customerId = ids(rnd.nextInt(ids.size))
        val eventType = eventTypes(rnd.nextInt(eventTypes.size))
        generateWrapperRecord(customerId, eventType)
      }.toList

      writeAvroRecords(existingPath, partitionId, buffer, wrapperSchema)
      Iterator.empty
    }.count()

    // New customers (500K × 5 events)
    val newCustomers = spark.sparkContext.parallelize(1 to 800, numSlices = 32)
    newCustomers.mapPartitionsWithIndex { case (partitionId, part) =>
      val buffer = part.flatMap { _ =>
        val p = rnd.nextInt(8)
        val newCustomerId = f"${p}_9${rnd.nextInt(999999)}%06d"
        Seq(
          generateWrapperRecord(newCustomerId, "NAME"),
          generateWrapperRecord(newCustomerId, "ADDRESS"),
          generateWrapperRecord(newCustomerId, "ADDRESS"),
          generateWrapperRecord(newCustomerId, "IDENTIFICATION"),
          generateWrapperRecord(newCustomerId, "IDENTIFICATION")
        )
      }.toList

      writeAvroRecords(newPath, partitionId, buffer, wrapperSchema)
      Iterator.empty
    }.count()

    println("✅ Valid Avro container files written.")
    spark.stop()
  }
}
