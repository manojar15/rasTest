package rastest

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.avro._
import java.nio.file.{Files, Paths}
import scala.io.Source

object PartitionedAvroConsumer {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("PartitionedAvroConsumer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // 1. Load all avro files from existing and new folders
    val basePath = "avro_output"
    val folders = Seq("existing", "new").flatMap { sub =>
      val path = s"$basePath/$sub"
      val dir = new java.io.File(path)
      if (dir.exists && dir.isDirectory)
        dir.listFiles.filter(_.isDirectory).map(_.getPath)
      else Seq.empty
    }

    // 2. Read all Avro files (CustomerEvent) into DataFrame
    val allDF = folders.map(path => spark.read.format("avro").load(path)).reduce(_ union _)

    // 3. Load CustomerEvent schema
    val eventSchemaJson = Source.fromFile("src/main/avro/CustomerEvent.avsc").mkString
    val namePayloadSchemaJson = Source.fromFile("src/main/avro/NamePayload.avsc").mkString
    val addressPayloadSchemaJson = Source.fromFile("src/main/avro/AddressPayload.avsc").mkString
    val identificationPayloadSchemaJson = Source.fromFile("src/main/avro/IdentificationPayload.avsc").mkString

    // 4. Parse and extract payload
    val withDecoded = allDF
      .withColumn("header", $"header")
      .withColumn("payload_bytes", $"payload")
      .withColumn("event_type", $"header.event_type")
      .withColumn("tenant_id", $"header.tenant_id")
      .withColumn("customer_id", $"header.entity_id")
      .withColumn("logical_date", $"header.logical_date")
      .withColumn("event_timestamp", $"header.event_timestamp")

    val nameDF = withDecoded
      .filter($"event_type" === "NAME_CHANGED")
      .withColumn("payload_json", from_avro($"payload_bytes", namePayloadSchemaJson))
      .select($"tenant_id", $"customer_id", $"logical_date", $"event_timestamp",
        $"payload_json.first".as("firstName"),
        $"payload_json.middle".as("middleName"),
        $"payload_json.last".as("lastName")
      )

    val addressDF = withDecoded
      .filter($"event_type" === "ADDRESS_CHANGED")
      .withColumn("payload_json", from_avro($"payload_bytes", addressPayloadSchemaJson))
      .select($"tenant_id", $"customer_id", $"logical_date", $"event_timestamp",
        $"payload_json.type".as("type"),
        $"payload_json.street",
        $"payload_json.city",
        $"payload_json.postal_code",
        $"payload_json.country"
      )

    val idDF = withDecoded
      .filter($"event_type" === "IDENTIFICATION_CHANGED")
      .withColumn("payload_json", from_avro($"payload_bytes", identificationPayloadSchemaJson))
      .select($"tenant_id", $"customer_id", $"logical_date", $"event_timestamp",
        $"payload_json.type".as("id_type"),
        $"payload_json.number",
        $"payload_json.issuer"
      )

    // 5. Write to partitioned Parquet files
    nameDF
      .withColumn("partition_id", lit("name"))
      .write
      .mode("overwrite")
      .partitionBy("tenant_id", "partition_id", "logical_date")
      .parquet("parquet_output/name")

    addressDF
      .withColumn("partition_id", lit("address"))
      .write
      .mode("overwrite")
      .partitionBy("tenant_id", "partition_id", "logical_date")
      .parquet("parquet_output/address")

    idDF
      .withColumn("partition_id", lit("identification"))
      .write
      .mode("overwrite")
      .partitionBy("tenant_id", "partition_id", "logical_date")
      .parquet("parquet_output/identification")

    println("✅ Avro consumer completed successfully.")
    spark.stop()
  }
}
