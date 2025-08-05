package rastest

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.util.Random

import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._

object ParallelPartitionedAvroEventProducerDF {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Avro Event Producer with DataFrame")
      .master("local[8]")
      .config("spark.executor.memory", "16g")
      .config("spark.driver.memory", "16g")
      .getOrCreate()

    import spark.implicits._

    val tenantId = 42
    val logicalDate = LocalDate.parse("2025-04-03", DateTimeFormatter.ISO_DATE)
    val logicalDateDays = logicalDate.toEpochDay.toInt
    val eventTypes = Seq("NAME", "ADDRESS", "IDENTIFICATION")
    val outputPath = "customer/avro_output"

    // Load existing customers
    val customerMetaPath = "C:/Users/e5655076/RAS_RPT/obrandrastest/customer/customer_metadata"
    val existingCustomerIds = spark.read.parquet(customerMetaPath)
      .select("customer_id").as[String].collect().toSet
    val broadcastCustomerIds = spark.sparkContext.broadcast(existingCustomerIds.toIndexedSeq)

    val existingEvents = spark.sparkContext.parallelize(1 to 4000000, numSlices = 64)

    val exEventsRDD = existingEvents.map { _ =>
      val rnd = new Random()
      val customerId = broadcastCustomerIds.value(rnd.nextInt(broadcastCustomerIds.value.size))
      val eventType = eventTypes(rnd.nextInt(eventTypes.size))

      val payload: String = eventType match {
        case "NAME" => s"{first: 'UpdatedFirst', last: 'UpdatedLast'}"
        case "ADDRESS" => s"{street: 'NewStreet', city: 'NewCity'}"
        case "IDENTIFICATION" => s"{type: 'passport', number: 'NEWID'}"
      }

      (System.currentTimeMillis(), logicalDateDays, eventType, tenantId, customerId, payload)
    }

    val existingDF = exEventsRDD.toDF(
      "event_timestamp", "logical_date", "event_type",
      "tenant_id", "entity_id", "payload"
    )

    // ---------- New customers ----------
    val newCustomerRange = spark.sparkContext.parallelize(1 to 500000, numSlices = 32)

    val newEventsRDD = newCustomerRange.flatMap { _ =>
      val rnd = new Random()
      val partitionId = rnd.nextInt(8)
      val customerId = f"${partitionId}_9${rnd.nextInt(999999)}%06d"

      Seq(
        ("NAME", s"{first: 'NewFirst_$customerId'}"),
        ("ADDRESS", s"{city: 'City_$customerId'}"),
        ("ADDRESS", s"{city: 'City2_$customerId'}"),
        ("IDENTIFICATION", s"{number: 'ID1_$customerId'}"),
        ("IDENTIFICATION", s"{number: 'ID2_$customerId'}")
      ).map { case (eventType, payload) =>
        (
          System.currentTimeMillis(),
          logicalDateDays,
          eventType,
          tenantId,
          customerId,
          payload
        )
      }
    }

    val newDF = newEventsRDD.toDF(
      "event_timestamp", "logical_date", "event_type",
      "tenant_id", "entity_id", "payload"
    )

    // Union both DataFrames
    val finalDF = existingDF.union(newDF)

    // Save result as Avro
    finalDF.write
      .format("avro")
      .mode("overwrite")
      .save(outputPath)

    println(s"✅ Events written to Avro format at folder: $outputPath")
    spark.stop()
  }
}
