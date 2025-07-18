package rastest

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.nio.file.{Files, Paths}
import java.sql.Date
import scala.util.Random

object PartitionedCustomerGenerator {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Partitioned Customer Generator")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val outputPath = "C:/Users/e5655076/RAS_RPT/obrandrastest/customer/customer_snapshot_avro"
    val metadataPath = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/customer_metadata"
    val numPartitions = 8
    val customersPerPartition = 2000
    val totalCustomers = numPartitions * customersPerPartition

    val schemaPath = "src/main/avro/CustomerRecord.avsc"
    val avroSchemaString = new String(Files.readAllBytes(Paths.get(schemaPath)))

    val date = Date.valueOf("2025-03-30")

    val df = spark.range(totalCustomers).repartition(numPartitions).mapPartitions { iter =>
      val rnd = new Random()
      iter.map { i =>
        val partitionId = (i / customersPerPartition).toInt
        val customerId = f"${partitionId}_$i%07d"
        val name = Name(s"First_$i", s"Last_$i", s"Middle_$i")
        val addresses = Seq(
          Address(AddressType.HOME, s"${i} Main St", s"City_$partitionId", f"10${partitionId}%03d", "CountryX"),
          Address(AddressType.WORK, s"${i} Work Ave", s"WorkCity_$partitionId", f"20${partitionId}%03d", "CountryY")
        )
        val ids = Seq(
          Identification("passport", s"P$i", "GovA"),
          Identification("driver_license", s"D$i", "GovB")
        )
        Customer("42", partitionId.toString, customerId, date, name, addresses, ids)
      }
    }.toDF()

    val outputDF = df.withColumn("x_tenant_id", col("tenant_id"))
      .withColumn("x_logical_date", col("logical_date"))

    // Write full customer data
    outputDF.write
      .mode(SaveMode.Overwrite)
      .format("avro")
      .partitionBy("x_tenant_id", "x_logical_date", "partition_id")
      .option("avroSchema", avroSchemaString)
      .save(outputPath)

    // write only customer id
    val idOnly = df.select("customer_id")
    idOnly.write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .save(metadataPath)


    println(s"✅ Generated $totalCustomers customers into $outputPath with dummy data")
    println(s"✅ Customer metadata saved to $metadataPath")

    spark.stop()
  }
}
