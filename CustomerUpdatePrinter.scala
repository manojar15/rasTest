package rastest

import org.apache.spark.sql.{Dataset, SparkSession}
import java.io.{BufferedWriter, FileWriter}
import java.time.format.DateTimeFormatter

object CustomerUpdatePrinter {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Print Customer Updates")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val inputPath = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/customerupdates/logical_date=2025-04-03"
    val outputPath = "C:/Users/e5655076/RAS_RPT/obrandrastest/customer/reports/customer_updates_report.txt"

    val updates: Dataset[CustomerUpdate] = spark.read
      .format("avro")
      .load(inputPath)
      .as[CustomerUpdate]

    val count = updates.count()
    println(s"Update Count: $count")


    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    new java.io.File(outputPath).getParentFile.mkdirs()
    val writer = new BufferedWriter(new FileWriter(outputPath, false))

    try {
      val collectedUpdates = updates.collect()
      writer.write(s"Total Updates: ${collectedUpdates.length}\n")
      writer.write("==================================================\n")

      collectedUpdates.foreach { update =>
        writer.write(s"Customer ID: ${update.customer_id}\n")

        if (update.oldName != null || update.newName != null) {
          writer.write("  Name change:\n")
          writer.write(s"    From: ${Option(update.oldName).getOrElse("<none>")}\n")
          writer.write(s"    To:   ${Option(update.newName).getOrElse("<none>")}\n")
        }

        if (update.oldAddress != null || update.newAddress != null) {
          writer.write("  Address change:\n")
          writer.write(s"    From: ${Option(update.oldAddress).getOrElse("<none>")}\n")
          writer.write(s"    To:   ${Option(update.newAddress).getOrElse("<none>")}\n")
        }

        if (update.oldId != null || update.newId != null) {
          writer.write("  Identification change:\n")
          writer.write(s"    From: ${Option(update.oldId).getOrElse("<none>")}\n")
          writer.write(s"    To:   ${Option(update.newId).getOrElse("<none>")}\n")
        }

        writer.write("--------------------------------------------------\n")
      }
    } finally {
      writer.close()
    }

    spark.stop()
  }
}


