package rastest

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.functions._

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

object SamplePartitionedEvents {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Sample Partitioned Events")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val basePath = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/tenant_data"
    val wrapperSchemaJson = new String(Files.readAllBytes(Paths.get("src/main/avro/CustomerEvent.avsc")), StandardCharsets.UTF_8)
    val namePayloadSchemaJson = new String(Files.readAllBytes(Paths.get("src/main/avro/NamePayload.avsc")), StandardCharsets.UTF_8)
    val addressPayloadSchemaJson = new String(Files.readAllBytes(Paths.get("src/main/avro/AddressPayload.avsc")), StandardCharsets.UTF_8)
    val idPayloadSchemaJson = new String(Files.readAllBytes(Paths.get("src/main/avro/IdentificationPayload.avsc")), StandardCharsets.UTF_8)

    val df = spark.read.format("avro").load(basePath)

    val filteredDF = df.take(10);

    val dfWithParsed = df
      .withColumn("event", from_avro(col("value"), wrapperSchemaJson))

    dfWithParsed.show(truncate = false)

    spark.stop()
  }
}
