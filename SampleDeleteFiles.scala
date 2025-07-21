package rastest

import org.apache.spark.sql.{SaveMode, SparkSession}

object SampleDeleteFiles {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("counts")
      .master("local[*]")
      .getOrCreate()

    val snapshotPath1 = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/avro_output"
    //val snapshotPath2 = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/avro_output/new"
    //val snapshotPath3 = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/customer_snapshot_avro"
    
    val epy = spark.createDataFrame(Seq.empty[(String,String,String)])
      .toDF("tenant_id","logical_date","partition_id")
    epy.write.mode(SaveMode.Overwrite).format("avro").save(snapshotPath1)
    //epy.write.mode(SaveMode.Overwrite).format("avro").save(snapshotPath2)
    spark.stop()
  }
}

