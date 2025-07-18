package rastest

import org.apache.spark.sql.SparkSession

object SampleTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("counts")
      .master("local[*]")
      .getOrCreate()

    val snapshotPath1 = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/avro_output/existing"
    val snapshotPath2 = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/avro_output/new"
    
    val df1 = spark.read.format("avro").load(snapshotPath1)
    df1.printSchema()
    df1.show(10,truncate=false)
    //println("count is",df1.count())


    val df2 = spark.read.format("avro").load(snapshotPath2)
    df2.printSchema()
    df2.show(10,truncate=false)
    //println("count is",df2.count())

    spark.stop()
  }
}

