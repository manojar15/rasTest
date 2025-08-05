package rastest

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.util.Random
import scala.io.Source

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumWriter}
import org.apache.avro.io.EncoderFactory
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object FileBasedAvroEventProducer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("File-based Avro Event Producer")
      .master("local[8]")
      .config("spark.sql.shuffle.partitions", "64")
      .getOrCreate()

    import spark.implicits._

    val tenantId = 42
    val logicalDateStr = "2025-04-03"
    val logicalDate = LocalDate.parse(logicalDateStr, DateTimeFormatter.ISO_DATE)
    val logicalDateDays = logicalDate.toEpochDay.toInt
    val eventTypes = Seq("NAME", "ADDRESS", "IDENTIFICATION")
    val outputPath = "customer/event_output"

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

    // Load existing customer IDs
    val customerMetaPath = "C:/Users/e5655076/RAS_RPT/obrandrastest/customer/customer_metadata"
    val existingCustomerIds = spark.read.parquet(customerMetaPath)
      .select("customer_id").as[String].collect().toSet
    val broadcastCustomerIds = spark.sparkContext.broadcast(existingCustomerIds.toIndexedSeq)

    // Helper: serialize Avro GenericRecord to bytes
    def serialize(avroSchema: Schema, record: GenericData.Record): Array[Byte] = {
      val out = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get().binaryEncoder(out, null)
      val writer = new GenericDatumWriter[GenericData.Record](avroSchema)
      writer.write(record, encoder)
      encoder.flush()
      out.close()
      out.toByteArray
    }

    // Generate serialized wrapped event bytes for a customer and event type
    def createWrappedEvent(customerId: String, eventType: String): Array[Byte] = {
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
          val t = if (new Random().nextBoolean()) "HOME" else "WORK"
          rec.put("type", new GenericData.EnumSymbol(addressSchema.getField("type").schema(), t))
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

      val payloadBytes = serialize(payloadSchema, payloadRecord)
      val wrapperRecord = new GenericData.Record(wrapperSchema)
      wrapperRecord.put("header", header)
      wrapperRecord.put("payload", ByteBuffer.wrap(payloadBytes))

      serialize(wrapperSchema, wrapperRecord)
    }

    // RDD: 4M events for existing customers
    val existingEventsRDD = spark.sparkContext.parallelize(1 to 4000000, 64).mapPartitions { iter =>
      val rnd = new Random()
      val customers = broadcastCustomerIds.value
      iter.map { _ =>
        val custId = customers(rnd.nextInt(customers.size))
        val evType = eventTypes(rnd.nextInt(eventTypes.size))
        val value = createWrappedEvent(custId, evType)
        val partitionId = custId.split("_")(0)
        Row(custId, evType, tenantId, logicalDateDays, System.currentTimeMillis(), partitionId, value)
      }
    }

    // RDD: 500K new customers, 5 events each
    val newCustomersRDD = spark.sparkContext.parallelize(1 to 500000, 32).flatMap { _ =>
      val rnd = new Random()
      val partitionId = rnd.nextInt(8)
      val custId = f"${partitionId}_9${rnd.nextInt(999999)}%06d"
      Seq("NAME", "ADDRESS", "ADDRESS", "IDENTIFICATION", "IDENTIFICATION").map { evType =>
        val value = createWrappedEvent(custId, evType)
        Row(custId, evType, tenantId, logicalDateDays, System.currentTimeMillis(), partitionId.toString, value)
      }
    }

    val schema = StructType(Seq(
      StructField("customer_id", StringType, nullable = false),
      StructField("event_type", StringType, nullable = false),
      StructField("tenant_id", IntegerType, nullable = false),
      StructField("logical_date", IntegerType, nullable = false),
      StructField("event_timestamp", LongType, nullable = false),
      StructField("partition_id", StringType, nullable = false),
      StructField("value", BinaryType, nullable = false)
    ))

    // Union events
    val allEventsRDD = existingEventsRDD.union(newCustomersRDD)

    val eventsDF = spark.createDataFrame(allEventsRDD, schema)

    eventsDF.write
      .partitionBy("tenant_id", "partition_id", "logical_date")
      .option("compression", "snappy")
      .mode("overwrite")
      .parquet(outputPath)

    println("✅ Event data successfully written as partitioned Parquet files.")
    spark.stop()
  }
}
