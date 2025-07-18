package rastest

import org.apache.avro._
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic._
import org.apache.avro.io.{EncoderFactory, DatumWriter, DatumReader, DecoderFactory}
import org.apache.spark.sql.SparkSession

import java.io.{ByteArrayOutputStream, File}
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

    def serializeToBytes(record: GenericRecord, schema: Schema): Array[Byte] = {
      val writer: DatumWriter[GenericRecord] = new GenericDatumWriter[GenericRecord](schema)
      val out = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get().binaryEncoder(out, null)
      writer.write(record, encoder)
      encoder.flush()
      out.toByteArray
    }

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

      val payloadBytes = serializeToBytes(payloadRecord, payloadSchema)
      val wrapper = new GenericData.Record(wrapperSchema)
      wrapper.put("header", header)
      wrapper.put("payload", java.nio.ByteBuffer.wrap(payloadBytes))

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

    // Existing customers (simulate 16K events, 64 partitions)
    spark.sparkContext.parallelize(1 to 16000, numSlices = 64)
      .mapPartitionsWithIndex { case (partitionId, part) =>
        val ids = broadcastCustomerIds.value
        val buffer = part.map { _ =>
          val customerId = ids(rnd.nextInt(ids.size))
          val eventType = Seq("NAME", "ADDRESS", "IDENTIFICATION")(rnd.nextInt(3))
          generateWrapperRecord(customerId, eventType)
        }.toList

        writeAvroRecords(existingPath, partitionId, buffer, wrapperSchema)
        Iterator.empty
      }.count()

    // New customers (simulate 4K customers × 5 events each, 32 partitions)
    spark.sparkContext.parallelize(1 to 800, numSlices = 32)
      .mapPartitionsWithIndex { case (partitionId, part) =>
        val buffer = part.flatMap { _ =>
          val newCustomerId = f"NEW_${rnd.nextInt(9999999)}%07d"
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

    println("✅ Avro container files written with original schema.")
    spark.stop()
  }
}
