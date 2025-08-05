package rastest

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.util.Random
import scala.io.Source

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions._

object ParallelPartitionedAvroEventProducerFile {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Parallel Partitioned Avro Event File Producer")
      .master("local[8]")
      .config("spark.sql.shuffle.partitions", "64")
      .getOrCreate()

    import spark.implicits._

    val tenantId = 42
    val logicalDate = LocalDate.parse("2025-04-03", DateTimeFormatter.ISO_DATE)
    val logicalDateDays = logicalDate.toEpochDay.toInt
    val eventTypes = Seq("NAME", "ADDRESS", "IDENTIFICATION")

    // Load Avro schemas (as before)
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

    // Generate 4 million events for existing customers
    val existingEventsRDD = spark.sparkContext.parallelize(1 to 4000000, 64).mapPartitions { part =>
      val rnd = new Random()
      val customers = broadcastCustomerIds.value

      part.map { _ =>
        val customerId = customers(rnd.nextInt(customers.size))
        val eventType = eventTypes(rnd.nextInt(eventTypes.size))
        val partitionId = customerId.split("_")(0)

        val header = new GenericData.Record(headerSchema)
        header.put("event_timestamp", System.currentTimeMillis())
        header.put("logical_date", logicalDateDays)
        header.put("event_type", eventType)
        header.put("tenant_id", tenantId)
        header.put("entity_id", customerId)

        val payload = eventType match {
          case "NAME" =>
            val rec = new GenericData.Record(nameSchema)
            rec.put("customer_id", customerId)
            rec.put("first", s"UpdatedFirst_$customerId")
            rec.put("middle", "Z")
            rec.put("last", s"UpdatedLast_$customerId")
            rec
          case "ADDRESS" =>
            val rec = new GenericData.Record(addressSchema)
            rec.put("customer_id", customerId)
            rec.put("type", if (rnd.nextBoolean()) "HOME" else "WORK")
            rec.put("street", s"NewStreet $customerId")
            rec.put("city", s"NewCity_$customerId")
            rec.put("postal_code", f"NewPC_${rnd.nextInt(999)}%03d")
            rec.put("country", "NewCountryX")
            rec
          case "IDENTIFICATION" =>
            val rec = new GenericData.Record(idSchema)
            rec.put("customer_id", customerId)
            rec.put("type", "passport")
            rec.put("number", s"NEWID_$customerId")
            rec.put("issuer", "GovX")
            rec
        }

        // Return a Row with fields matching the Avro wrapper schema in flattened way
        Row(
          header.get("event_timestamp").asInstanceOf[Long],
          header.get("logical_date").asInstanceOf[Int],
          header.get("event_type").toString,
          header.get("tenant_id").asInstanceOf[Int],
          header.get("entity_id").toString,
          payload
        )
      }
    }

    val wrapperSchemaStruct = org.apache.spark.sql.avro.SchemaConverters.toSqlType(wrapperSchema).dataType

    val eventsDF = spark.createDataFrame(existingEventsRDD, wrapperSchemaStruct)

    // Write Avro files partitioned by tenant_id, partition_id (derived), logical_date
    val outputPath = "customer/event_output"

    val dfWithPartition = eventsDF.withColumn("tenant_id", col("header.tenant_id"))
      .withColumn("partition_id", split(col("header.entity_id"), "_")(0))
      .withColumn("logical_date", col("header.logical_date"))

    dfWithPartition.write
      .partitionBy("tenant_id", "partition_id", "logical_date")
      .format("avro")
      .option("compression", "snappy")
      .mode("overwrite")
      .save(outputPath)

    println("Finished writing Avro event files for consumer job.")
    spark.stop()
  }
}
