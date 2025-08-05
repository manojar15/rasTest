package rastest

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DatumWriter, EncoderFactory}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import java.io.ByteArrayOutputStream
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.io.Source
import scala.util.Random

object PartitionedAvroEventProducerBatch {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Partitioned Avro Event Producer (Faithful, No Kafka)")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    val baseOutputPath = "C:/Users/e5655076/RAS_RPT/obrandrastest/customer/avro_output"
    val customerMetaPath = "C:/Users/e5655076/RAS_RPT/obrandrastest/customer/customer_metadata"
    val logicalDate = LocalDate.parse("2025-04-03", DateTimeFormatter.ISO_DATE)
    val tenantId = 42
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

    val customerIds = spark.read.parquet(customerMetaPath)
      .select("customer_id").as[String].collect().toSeq
    val random = new Random()

    def buildEvent(customerId: String, eventType: String, partitionId: String, isNew: Boolean = false): GenericRecord = {
      val eventTimestamp = System.currentTimeMillis()
      val header = new GenericData.Record(headerSchema)
      header.put("event_timestamp", eventTimestamp)
      header.put("logical_date", DateTimeFormatter.ISO_DATE.format(logicalDate))
      header.put("event_type", eventType)
      header.put("tenant_id", tenantId)
      header.put("entity_id", customerId)

      val (payloadSchema, payloadRecord) = eventType match {
        case "NAME" =>
          val rec = new GenericData.Record(nameSchema)
          rec.put("customer_id", customerId)
          rec.put("first", if (isNew) s"First_$customerId" else s"UpdatedFirst_$customerId")
          rec.put("middle", if (isNew) "X" else "Z")
          rec.put("last", if (isNew) s"Last_$customerId" else s"UpdatedLast_$customerId")
          (nameSchema, rec)
        case "ADDRESS" =>
          val typ = if (random.nextBoolean()) "HOME" else "WORK"
          val rec = new GenericData.Record(addressSchema)
          rec.put("customer_id", customerId)
          rec.put("type", new GenericData.EnumSymbol(addressSchema.getField("type").schema(), typ))
          rec.put("street", s"AddrStreet_$customerId")
          rec.put("city", s"AddrCity_$partitionId")
          rec.put("postal_code", f"Code_${random.nextInt(1000)}%03d")
          rec.put("country", if (isNew) "CountryY" else "NewCountryX")
          (addressSchema, rec)
        case "IDENTIFICATION" =>
          val rec = new GenericData.Record(idSchema)
          rec.put("customer_id", customerId)
          rec.put("type", "passport")
          rec.put("number", s"ID_$customerId")
          rec.put("issuer", if (isNew) "GovB" else "GovX")
          (idSchema, rec)
      }

      val wrapper = new GenericData.Record(wrapperSchema)
      wrapper.put("header", header)
      wrapper.put("payload", payloadRecord)
      wrapper
    }

    // Modified events (existing customers)
    val nModified = 16000
    val modifiedEvents = (1 to nModified).map { _ =>
      val idx = random.nextInt(customerIds.length)
      val customerId = customerIds(idx)
      val partitionId = customerId.split("_").headOption.getOrElse("0")
      val eventType = eventTypes(random.nextInt(eventTypes.length))
      buildEvent(customerId, eventType, partitionId, isNew = false)
    }

    // New customers: each gets multiple events (NAME, ADDRESS, ADDRESS, ID, ID)
    val nNewCustomers = 800
    val newEvents = (1 to nNewCustomers).flatMap { _ =>
      val partitionId = random.nextInt(8).toString
      val newCustomerId = f"${partitionId}_9${random.nextInt(999999)}%06d"
      Seq(
        buildEvent(newCustomerId, "NAME", partitionId, isNew = true),
        buildEvent(newCustomerId, "ADDRESS", partitionId, isNew = true),
        buildEvent(newCustomerId, "ADDRESS", partitionId, isNew = true),
        buildEvent(newCustomerId, "IDENTIFICATION", partitionId, isNew = true),
        buildEvent(newCustomerId, "IDENTIFICATION", partitionId, isNew = true)
      )
    }

    val sparkAvroStruct = org.apache.spark.sql.avro.SchemaConverters.toSqlType(wrapperSchema).dataType.asInstanceOf[StructType]

    def writeDataset(events: Seq[GenericRecord], folder: String): Unit = {
      val rdd = spark.sparkContext.parallelize(events)
      val df = spark.createDataFrame(rdd.map(Row(_)), StructType(Seq(StructField("event", sparkAvroStruct))))
        .select("event.*") // explode struct so every field is a column (header, payload)
      df.write
        .mode("overwrite")
        .format("avro")
        .save(s"$baseOutputPath/$folder")
    }

    writeDataset(modifiedEvents, "modified")
    writeDataset(newEvents, "new")

    println(s"✅ Modified and New customer events written (as Avro) in: $baseOutputPath/modified and $baseOutputPath/new")
    spark.stop()
  }
}
