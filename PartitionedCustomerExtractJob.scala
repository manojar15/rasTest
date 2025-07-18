package rastest

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.io.File

object CustomerExtractJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Extract Customer Data")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val extractMode = args(0) // "delta" or "full"
    val tenantId = args(1)
    val fromDate = args(2) // e.g., "2025-03-30"
    val toDate = args(3)   // e.g., "2025-04-01"

    val basePath = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer"
    val deltaPath = s"$basePath/customer_snapshot_delta"
    val snapshotPath = s"$basePath/initial_customers_avro"

    val dateRange = getDateRange(fromDate, toDate)

    val snapshot = if (extractMode == "full") {
      val base = spark.read.format("avro").load(snapshotPath)
      val deltas = dateRange.flatMap(date => loadIfExists(spark, s"$deltaPath/logical_date=$date"))
      deltas.foldLeft(base)((acc, delta) => acc.unionByName(delta).dropDuplicates("customer_id"))
    } else {
      dateRange.flatMap(date => loadIfExists(spark, s"$deltaPath/logical_date=$date")).reduce(_ unionByName _)
    }

    val exploded = snapshot.flatMap { row =>
      val customerId = Option(row.getAs[String]("customer_id")).getOrElse("")

      val nameStruct = Option(row.getStruct(row.fieldIndex("name")))
      val nameLine = s"1|$customerId|" +
        nameStruct.flatMap(n => Option(n.getString(0))).getOrElse("") + "|" +
        nameStruct.flatMap(n => Option(n.getString(1))).getOrElse("") + "|" +
        nameStruct.flatMap(n => Option(n.getString(2))).getOrElse("")

      val addressLines = Option(row.getSeq[Row](row.fieldIndex("addresses"))).getOrElse(Seq.empty).map { addr =>
        val line = Seq.tabulate(5)(i => Option(addr.getString(i)).getOrElse("")).mkString("|")
        s"2|$customerId|$line"
      }

      val idLines = Option(row.getSeq[Row](row.fieldIndex("identifications"))).getOrElse(Seq.empty).map { id =>
        val line = Seq.tabulate(3)(i => Option(id.getString(i)).getOrElse("")).mkString("|")
        s"3|$customerId|$line"
      }

      Seq(nameLine) ++ addressLines ++ idLines
    }

    exploded.rdd.coalesce(1)
      .saveAsTextFile(s"$basePath/extract_output/tenant_id=$tenantId")

    spark.stop()
  }

  def getDateRange(from: String, to: String): Seq[String] = {
    val fromDate = java.time.LocalDate.parse(from)
    val toDate = java.time.LocalDate.parse(to)
    Iterator.iterate(fromDate)(_.plusDays(1)).takeWhile(!_.isAfter(toDate)).map(_.toString).toSeq
  }

  def loadIfExists(spark: SparkSession, path: String): Option[DataFrame] = {
    val localPath = path.stripPrefix("file://")
    val file = new File(localPath)
    if (file.exists && file.isDirectory) Some(spark.read.format("avro").load(path)) else None
  }
}


