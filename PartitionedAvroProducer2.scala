package rastest

import org.apache.spark.sql.SparkSession
import org.apache.avro.Schema
import org.apache.avro.generic._
import org.apache.avro.io.{DatumWriter, EncoderFactory}
import org.apache.avro.file.DataFileWriter
import java.io.File
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.util.Random

object FileBasedAvroEventProducer {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("File Based Avro Event Producer")
      .master("local[8]")
      .getOrCreate()

    import spark.implicits._

    // Load customer metadata IDs (like broadcast in original)
    val customerMetaPath = "C:/Users/e5655076/RAS_RPT/obrandrastest/customer/customer_metadata"
    val existingCustomerIds = spark.read.parquet(customerMetaPath)
      .select("customer_id").as[String].collect().toIndexedSeq

    val broadcastCustomerIds = spark.sparkContext.broadcast(existingCustomerIds)

    // Load Avro schemas (same as original)
    val wrapperSchemaJson = new String(Files.readAllBytes(Paths.get("src/main/avro/CustomerEvent.avsc")), StandardCharsets.UTF_8)
    val nameSchemaJson = new String(Files.readAllBytes(Paths.get("src/main/avro/NamePayload.avsc")), StandardCharsets.UTF_8)
    val addressSchemaJson = new String(Files.readAllBytes(Paths.get("src/main/avro/AddressPayload.avsc")), StandardCharsets.UTF_8)
    val idSchemaJson = new String(Files.readAllBytes(Paths.get("src/main/avro/IdentificationPayload.avsc")), StandardCharsets.UTF_8)

    val wrapperSchema = new Schema.Parser().parse(wrapperSchemaJson)
    val headerSchema = wrapperSchema.getField("header").schema()
    val nameSchema = new Schema.Parser().parse(nameSchemaJson)
    val addressSchema = new Schema.Parser().parse(addressSchemaJson)
    val idSchema = new Schema.Parser().parse(idSchemaJson)

    val tenantId = 42
    val logicalDate = LocalDate.parse("2025-04-03", DateTimeFormatter.ISO_DATE)
    val logicalDateDays = logicalDate.toEpochDay.toInt
    val eventTypes = Seq("NAME", "ADDRESS", "IDENTIFICATION")

    val avroOutputBase = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/avro_events"

    // Existing customers event generation count, parallelism etc.
    val totalExistingEvents = 4000000
    val existingEventPartitions = 64

    // New customers count and events per new customer
    val newCustomerCount = 500000
    val newEventPartitions = 32

    // Function to generate event GenericRecord
    def generateEvent(customerId: String, eventType: String, random: Random): GenericRecord = {
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
          val addrType = if (random.nextBoolean()) "HOME" else "WORK"
          rec.put("type", new GenericData.EnumSymbol(addressSchema.getField("type").schema(), addrType))
          rec.put("street", s"NewStreet $customerId")
          rec.put("city", s"NewCity_$customerId")
          rec.put("postal_code", f"NewPC_${random.nextInt(999)}%03d")
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

      val payloadOut = new java.io.ByteArrayOutputStream()
      val payloadEncoder = EncoderFactory.get().binaryEncoder(payloadOut, null)
      val payloadWriter = new org.apache.avro.generic.GenericDatumWriter[GenericRecord](schema)
      payloadWriter.write(payload, payloadEncoder)
      payloadEncoder.flush()
      payloadOut.close()

      val wrapper = new GenericData.Record(wrapperSchema)
      wrapper.put("header", header)
      wrapper.put("payload", java.nio.ByteBuffer.wrap(payloadOut.toByteArray))

      wrapper
    }

    // Write a list of GenericRecords to an Avro file (with wrapperSchema)
    def writeAvroFile(records: Seq[GenericRecord], tenantId: Int, partitionId: Int, logicalDate: String): Unit = {
      val outputDir = Paths.get(avroOutputBase, s"tenant_id=$tenantId", s"partition_id=$partitionId", s"logical_date=$logicalDate").toFile
      if (!outputDir.exists()) outputDir.mkdirs()
      val outFile = new File(outputDir, s"part-${System.nanoTime()}.avro")

      val datumWriter = new org.apache.avro.generic.GenericDatumWriter[GenericRecord](wrapperSchema)
      val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
      dataFileWriter.create(wrapperSchema, outFile)
      records.foreach(dataFileWriter.append)
      dataFileWriter.close()
    }

    // Generate events for existing customers
    spark.sparkContext.parallelize(1 to totalExistingEvents, existingEventPartitions).foreachPartition { iter =>
      val rnd = new Random()
      val customerIds = broadcastCustomerIds.value
      val eventBuf = scala.collection.mutable.ArrayBuffer.empty[GenericRecord]

      iter.foreach { _ =>
        val custId = customerIds(rnd.nextInt(customerIds.size))
        val eventType = eventTypes(rnd.nextInt(eventTypes.size))
        val eventRecord = generateEvent(custId, eventType, rnd)
        eventBuf += eventRecord

        // write per 1000 events or end of partition
        if (eventBuf.size >= 1000) {
          val partitionId = custId.split("_")(0).toInt
          writeAvroFile(eventBuf.toSeq, tenantId, partitionId, logicalDate.toString)
          eventBuf.clear()
        }
      }
      // flush leftovers
      if (eventBuf.nonEmpty) {
        val partitionId = eventBuf.head.get("header").asInstanceOf[GenericRecord].get("entity_id").toString.split("_")(0).toInt
        writeAvroFile(eventBuf.toSeq, tenantId, partitionId, logicalDate.toString)
        eventBuf.clear()
      }
    }

    // Generate events for new customers (500,000) - 5 events per customer
    spark.sparkContext.parallelize(1 to newCustomerCount, newEventPartitions).foreachPartition { iter =>
      val rnd = new Random()
      val eventBuf = scala.collection.mutable.ArrayBuffer.empty[GenericRecord]

      iter.foreach { i =>
        val partitionId = rnd.nextInt(8)
        val newCustId = f"${partitionId}_9${rnd.nextInt(999999)}%06d"

        // As original: 5 events - 1 NAME, 2 ADDRESS, 2 IDENTIFICATION
        val events = Seq(
          ("NAME", 1),
          ("ADDRESS", 2),
          ("IDENTIFICATION", 2)
        ).flatMap { case (etype, count) =>
          Seq.fill(count)(generateEvent(newCustId, etype, rnd))
        }

        eventBuf ++= events

        if (eventBuf.size >= 1000) {
          writeAvroFile(eventBuf.toSeq, tenantId, partitionId, logicalDate.toString)
          eventBuf.clear()
        }
      }
      if (eventBuf.nonEmpty) {
        writeAvroFile(eventBuf.toSeq, tenantId, eventBuf.head.get("header").asInstanceOf[GenericRecord].get("entity_id").toString.split("_")(0).toInt, logicalDate.toString)
        eventBuf.clear()
      }
    }

    println("Finished generating Avro event files.")
    spark.stop()
  }
}
