package rastest

import java.io.File
import java.util.UUID
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.nio.ByteBuffer

import org.apache.avro.Schema
import org.apache.avro.file.{DataFileWriter, CodecFactory}
import org.apache.avro.generic.{GenericDatumWriter, GenericData, GenericRecord}
import org.apache.spark.sql.SparkSession

import scala.io.Source
import scala.util.Random

object ParallelPartitionedAvroEventProducer {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Parallel Partitioned Avro Event Producer")
      .master("local[8]")
      .config("spark.executor.memory", "16g")
      .config("spark.driver.memory", "16g")
      .config("spark.sql.shuffle.partitions", "64")
      .getOrCreate()

    import spark.implicits._

    val tenantId = 42
    val logicalDate = LocalDate.parse("2025-04-03", DateTimeFormatter.ISO_DATE)
    val logicalDateDays = logicalDate.toEpochDay.toInt
    val eventTypes = Seq("NAME", "ADDRESS", "IDENTIFICATION")
    val outputDir = "customer/avro_output"

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
    val existingCustomerIds = spark.read.parquet(customerMetaPath)
      .select("customer_id").as[String].collect().toSet
    val broadcastCustomerIds = spark.sparkContext.broadcast(existingCustomerIds.toIndexedSeq)

    val fs = new File(outputDir)
    if (!fs.exists()) fs.mkdirs()

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
          rec.put("type", new GenericData.EnumSymbol(
            addressSchema.getField("type").schema(),
            if (new Random().nextBoolean()) "HOME" else "WORK"
          ))
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

      val outStream = new java.io.ByteArrayOutputStream()
      val encoder = org.apache.avro.io.EncoderFactory.get().binaryEncoder(outStream, null)
      val writer = new GenericDatumWriter[GenericRecord](schema)
      writer.write(payload, encoder)
      encoder.flush()
      outStream.close()

      val wrapper = new GenericData.Record(wrapperSchema)
      wrapper.put("header", header)
      wrapper.put("payload", ByteBuffer.wrap(outStream.toByteArray))

      wrapper
    }

    def writeAvroPartition(records: Iterator[GenericRecord], prefix: String): Unit = {
      if (records.hasNext) {
        val uuid = UUID.randomUUID().toString
        val file = new File(s"$outputDir/$prefix-$uuid.avro")
        val datumWriter = new GenericDatumWriter[GenericRecord](wrapperSchema)
        val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
        dataFileWriter.setCodec(CodecFactory.snappyCodec())
        dataFileWriter.create(wrapperSchema, file)

        records.foreach(dataFileWriter.append)
        dataFileWriter.close()
      }
    }

    // ----------- Existing: 4M events ------------
    val existingEvents = spark.sparkContext.parallelize(1 to 4000000, 64)

    existingEvents
      .mapPartitions { part =>
        val rnd = new Random()
        val customers = broadcastCustomerIds.value
        part.map { _ =>
          val customerId = customers(rnd.nextInt(customers.size))
          val eventType = eventTypes(rnd.nextInt(eventTypes.size))
          generateEvent(customerId, eventType)
        }
      }
      .foreachPartition(part => writeAvroPartition(part, "existing"))

    // ----------- New customers: 500K × 5 events ------------
    val newCustomerRange = spark.sparkContext.parallelize(1 to 500000, 32)

    newCustomerRange
      .mapPartitions { part =>
        val rnd = new Random()
        part.flatMap { _ =>
          val partitionId = rnd.nextInt(8)
          val newCustomerId = f"${partitionId}_9${rnd.nextInt(999999)}%06d"
          Seq(
            generateEvent(newCustomerId, "NAME"),
            generateEvent(newCustomerId, "ADDRESS"),
            generateEvent(newCustomerId, "ADDRESS"),
            generateEvent(newCustomerId, "IDENTIFICATION"),
            generateEvent(newCustomerId, "IDENTIFICATION")
          )
        }
      }
      .foreachPartition(part => writeAvroPartition(part, "new"))

    println("✅ Finished writing Avro files to customer/avro_output/")
    spark.stop()
  }
}
