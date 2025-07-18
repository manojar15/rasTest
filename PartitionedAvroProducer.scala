package rastest

import java.io.{ByteArrayOutputStream, File, FileOutputStream}
import java.nio.ByteBuffer
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.io.Source
import scala.util.Random

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, DatumWriter, EncoderFactory}
import org.apache.avro.generic.GenericDatumWriter
import org.apache.spark.sql.{SparkSession, SaveMode}

object FileBasedAvroEventProducer {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("File Based Avro Event Producer")
      .master("local[*]")
      .getOrCreate()

    val logicalDate = LocalDate.parse("2025-04-03", DateTimeFormatter.ISO_DATE)
    val logicalDateStr = logicalDate.toString // yyyy-MM-dd
    val logicalDateDays = logicalDate.toEpochDay.toInt
    val tenantId = 42
    val eventTypes = Seq("NAME", "ADDRESS", "IDENTIFICATION")

    // Load schemas
    val wrapperSchema = new Schema.Parser().parse(Source.fromFile("src/main/avro/CustomerEvent.avsc").mkString)
    val headerSchema = wrapperSchema.getField("header").schema()
    val nameSchema = new Schema.Parser().parse(Source.fromFile("src/main/avro/NamePayload.avsc").mkString)
    val addressSchema = new Schema.Parser().parse(Source.fromFile("src/main/avro/AddressPayload.avsc").mkString)
    val idSchema = new Schema.Parser().parse(Source.fromFile("src/main/avro/IdentificationPayload.avsc").mkString)

    // Load existing customer IDs
    val existingCustomerIds = spark.read.parquet("C:/Users/e5655076/RAS_RPT/obrandrastest/customer/customer_metadata")
      .select("customer_id").as[String].collect().toIndexedSeq
    val broadcastCustomerIds = spark.sparkContext.broadcast(existingCustomerIds)

    val baseOutputPath = s"avro_output/$logicalDateStr"
    val existingPath = s"$baseOutputPath/existing"
    val newPath = s"$baseOutputPath/new"

    val rnd = new Random()

    def serializeEvent(record: GenericRecord, schema: Schema): Array[Byte] = {
      val out = new ByteArrayOutputStream()
      val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)
      val writer: DatumWriter[GenericRecord] = new GenericDatumWriter[GenericRecord](schema)
      writer.write(record, encoder)
      encoder.flush()
      out.toByteArray
    }

    def generateWrapper(customerId: String, eventType: String): Array[Byte] = {
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

      val serializedPayload = serializeEvent(payloadRecord, payloadSchema)
      val wrapper = new GenericData.Record(wrapperSchema)
      wrapper.put("header", header)
      wrapper.put("payload", ByteBuffer.wrap(serializedPayload))

      serializeEvent(wrapper, wrapperSchema)
    }

    def writeAvroRecords(outputDir: String, partitionId: Int, records: Seq[Array[Byte]]): Unit = {
      val outPath = s"$outputDir/events_partition_$partitionId.avro"
      val outFile = new File(outPath)
      outFile.getParentFile.mkdirs()
      val fos = new FileOutputStream(outFile)
      records.foreach(fos.write)
      fos.close()
    }

    // --- Existing Customers: 4M events ---
    val existingEvents = spark.sparkContext.parallelize(1 to 4000000, numSlices = 64)
    existingEvents.mapPartitionsWithIndex { case (partitionId, part) =>
      val ids = broadcastCustomerIds.value
      val buffer = part.map { _ =>
        val customerId = ids(rnd.nextInt(ids.size))
        val eventType = eventTypes(rnd.nextInt(eventTypes.size))
        generateWrapper(customerId, eventType)
      }.toList
      writeAvroRecords(existingPath, partitionId, buffer)
      Iterator.empty
    }.count() // force execution

    // --- New Customers: 500K customers, 5 events each ---
    val newCustomers = spark.sparkContext.parallelize(1 to 500000, numSlices = 32)
    newCustomers.mapPartitionsWithIndex { case (partitionId, part) =>
      val buffer = part.flatMap { i =>
        val p = rnd.nextInt(8)
        val newCustomerId = f"${p}_9${rnd.nextInt(999999)}%06d"
        Seq(
          generateWrapper(newCustomerId, "NAME"),
          generateWrapper(newCustomerId, "ADDRESS"),
          generateWrapper(newCustomerId, "ADDRESS"),
          generateWrapper(newCustomerId, "IDENTIFICATION"),
          generateWrapper(newCustomerId, "IDENTIFICATION")
        )
      }.toList
      writeAvroRecords(newPath, partitionId, buffer)
      Iterator.empty
    }.count() // force execution

    println("✅ Avro event files written successfully.")
    spark.stop()
  }
}
