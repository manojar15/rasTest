package rastest

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DatumWriter, EncoderFactory}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import java.io.ByteArrayOutputStream
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.io.Source
import scala.util.Random

object AvroEventProducerWithPartitionedFolder {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Partitioned Avro Event Producer (File)")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    val baseOutputPath = "C:/Users/e5655076/RAS_RPT/obrandrastest/customer/avro_output"
    val customerMetaPath = "C:/Users/e5655076/RAS_RPT/obrandrastest/customer/customer_metadata"
    val logicalDate = LocalDate.parse("2025-04-03", DateTimeFormatter.ISO_DATE)
    val logicalDateStr = logicalDate.format(DateTimeFormatter.ISO_DATE)  // "2025-04-03"
    val logicalDateInt = logicalDate.toEpochDay.toInt                    // 20181
    val tenantId = 42
    val eventTypes = Seq("NAME", "ADDRESS", "IDENTIFICATION")

    // Avro schema loading
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

    def buildEvent(
                    customerId: String,
                    eventType: String,
                    partitionId: String,
                    isNew: Boolean = false
                  ): (String, Int, String, Int, String, Array[Byte], Array[Byte], String, Long) = {
      val eventTimestamp = System.currentTimeMillis()
      val header = new GenericData.Record(headerSchema)
      header.put("event_timestamp", eventTimestamp)
      header.put("logical_date", logicalDateInt)
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
          val rec = new GenericData.Record(addressSchema)
          rec.put("customer_id", customerId)
          rec.put("type", new GenericData.EnumSymbol(
            addressSchema.getField("type").schema(),
            if (random.nextBoolean()) "HOME" else "WORK"
          ))
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

      def serialize(schema: Schema, record: GenericRecord): Array[Byte] = {
        val out = new ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(out, null)
        val writer: DatumWriter[GenericRecord] = new GenericDatumWriter[GenericRecord](schema)
        writer.write(record, encoder)
        encoder.flush()
        out.close()
        out.toByteArray
      }
      val payloadBytes = serialize(payloadSchema, payloadRecord)
      val wrapper = new GenericData.Record(wrapperSchema)
      wrapper.put("header", header)
      wrapper.put("payload", java.nio.ByteBuffer.wrap(payloadBytes))
      val wrapperBytes = serialize(wrapperSchema, wrapper)
      (
        partitionId, tenantId, customerId, logicalDateInt, eventType,
        payloadBytes, wrapperBytes, logicalDateStr, eventTimestamp
      )
    }

    val nModified = 16000
    val modifiedEvents = spark.sparkContext.parallelize(1 to nModified, numSlices = 64).map { _ =>
      val idx = random.nextInt(customerIds.length)
      val customerId = customerIds(idx)
      val partitionId = customerId.split("_").headOption.getOrElse("0")
      val eventType = eventTypes(random.nextInt(eventTypes.length))
      buildEvent(customerId, eventType, partitionId, isNew = false)
    }

    val nNewCustomers = 800
    val newEvents = spark.sparkContext.parallelize(1 to nNewCustomers, numSlices = 32).flatMap { _ =>
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

    val eventSchema = StructType(Seq(
      StructField("partition_id", StringType),
      StructField("tenant_id", IntegerType),
      StructField("customer_id", StringType),
      StructField("logical_date_int", IntegerType),
      StructField("event_type", StringType),
      StructField("payload_bytes", BinaryType),
      StructField("wrapper_bytes", BinaryType),
      StructField("logical_date", StringType),   // for partitioning
      StructField("event_timestamp", LongType)   // required by merge job
    ))

    val modifiedDF = spark.createDataFrame(modifiedEvents.map(Row.fromTuple), eventSchema)
    val newDF = spark.createDataFrame(newEvents.map(Row.fromTuple), eventSchema)

    modifiedDF
      .write
      .mode("overwrite")
      .partitionBy("tenant_id", "partition_id", "logical_date")
      .format("avro")
      .save(s"$baseOutputPath/modified")

    newDF
      .write
      .mode("overwrite")
      .partitionBy("tenant_id", "partition_id", "logical_date")
      .format("avro")
      .save(s"$baseOutputPath/new")

    println(s"✅ Wrote Avro events: $baseOutputPath/modified and $baseOutputPath/new, partitioned with logical_date=$logicalDateStr and event_timestamp as top-level column!")

    spark.stop()
  }
}