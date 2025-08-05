package rastest

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.io.Source
import scala.util.Random

object ParallelPartitionedAvroProducerFile {

  def main(args: Array[String]): Unit = {
    // Spark session config - same as your original job
    val spark = SparkSession.builder()
      .appName("Parallel Partitioned Avro Event Producer - File Output")
      .master("local[8]")
      .config("spark.sql.shuffle.partitions", "64")
      .config("spark.executor.memory", "16g")
      .config("spark.driver.memory", "16g")
      .getOrCreate()

    import spark.implicits._

    // Constants (same as original)
    val tenantId = 42
    val logicalDate = LocalDate.parse("2025-04-03", DateTimeFormatter.ISO_DATE)
    val logicalDateDays = logicalDate.toEpochDay.toInt
    val eventTypes = Seq("NAME", "ADDRESS", "IDENTIFICATION")

    val outputPath = "customer/event_output" // output folder to replace Kafka

    // Load Avro schemas (same as original)
    val wrapperSchemaStr = Source.fromFile("src/main/avro/CustomerEvent.avsc").mkString
    val nameSchemaStr = Source.fromFile("src/main/avro/NamePayload.avsc").mkString
    val addressSchemaStr = Source.fromFile("src/main/avro/AddressPayload.avsc").mkString
    val idSchemaStr = Source.fromFile("src/main/avro/IdentificationPayload.avsc").mkString

    val wrapperSchema = new Schema.Parser().parse(wrapperSchemaStr)
    val headerSchema = wrapperSchema.getField("header").schema()
    val nameSchema = new Schema.Parser().parse(nameSchemaStr)
    val addressSchema = new Schema.Parser().parse(addressSchemaStr)
    val idSchema = new Schema.Parser().parse(idSchemaStr)

    // Load customer IDs from your metadata parquet (same as original)
    val customerMetaPath = "C:/Users/e5655076/RAS_RPT/obrandrastest/customer/customer_metadata"
    val existingCustomerIds = spark.read.parquet(customerMetaPath)
      .select("customer_id").as[String].collect().toSet
    val broadcastCustomerIds = spark.sparkContext.broadcast(existingCustomerIds.toIndexedSeq)

    // Serialize Avro record helper (same as your original code)
    def serializeRecord(schema: Schema, record: GenericData.Record): Array[Byte] = {
      val baos = new ByteArrayOutputStream()
      val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(baos, null)
      val writer = new GenericDatumWriter[GenericRecord](schema)
      writer.write(record, encoder)
      encoder.flush()
      baos.close()
      baos.toByteArray
    }

    // Build wrapped Avro event bytes exactly as your generateAndSendEvent
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
          val addrType = if (new Random().nextBoolean()) "HOME" else "WORK"
          rec.put("type", new GenericData.EnumSymbol(addressSchema.getField("type").schema(), addrType))
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

      val payloadBytes = serializeRecord(payloadSchema, payloadRecord)

      val wrapperRecord = new GenericData.Record(wrapperSchema)
      wrapperRecord.put("header", header)
      wrapperRecord.put("payload", ByteBuffer.wrap(payloadBytes))

      serializeRecord(wrapperSchema, wrapperRecord)
    }

    // Prepare RDD of events for existing customers (4M events)
    val existingEventsRDD = spark.sparkContext.parallelize(1 to 16000, 64).mapPartitions { part =>
      val rnd = new Random()
      val customers = broadcastCustomerIds.value
      part.map { _ =>
        val custId = customers(rnd.nextInt(customers.size))
        val evType = eventTypes(rnd.nextInt(eventTypes.size))
        val value = createWrappedEvent(custId, evType)
        val partitionId = custId.split("_")(0)
        Row(custId, evType, tenantId, logicalDateDays, System.currentTimeMillis(), partitionId, value)
      }
    }

    // Prepare RDD of events for new customers (500K customers x 5 events each)
    val newCustomersRDD = spark.sparkContext.parallelize(1 to 800, 32).flatMap { _ =>
      val rnd = new Random()
      val partitionId = rnd.nextInt(8)
      val custId = f"${partitionId}_9${rnd.nextInt(999999)}%06d"
      Seq("NAME", "ADDRESS", "ADDRESS", "IDENTIFICATION", "IDENTIFICATION").map { evType =>
        val value = createWrappedEvent(custId, evType)
        Row(custId, evType, tenantId, logicalDateDays, System.currentTimeMillis(), partitionId.toString, value)
      }
    }

    import org.apache.spark.sql.types._

    // Define schema identical to Kafka consumer output
    val schema = StructType(Seq(
      StructField("customer_id", StringType, nullable = false),
      StructField("event_type", StringType, nullable = false),
      StructField("tenant_id", IntegerType, nullable = false),
      StructField("logical_date", IntegerType, nullable = false),
      StructField("event_timestamp", LongType, nullable = false),
      StructField("partition_id", StringType, nullable = false),
      StructField("value", BinaryType, nullable = false)
    ))

    val allEventsRDD = existingEventsRDD.union(newCustomersRDD)
    val eventsDF = spark.createDataFrame(allEventsRDD, schema)

    // Write partitioned Parquet files in exact same structure and types as Kafka consumer output
    eventsDF.write
      .partitionBy("tenant_id", "partition_id", "logical_date")
      .option("compression", "snappy")
      .mode("overwrite")
      .parquet(outputPath)

    println("✅ Producer job finished writing file-based events")

    spark.stop()
  }
}