package customer.consumer

import org.apache.avro.Schema import org.apache.avro.generic.{GenericDatumReader, GenericRecord} import org.apache.avro.io.{DecoderFactory} import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession} import org.apache.spark.sql.functions._ import java.nio.file.{Files, Paths} import java.time.LocalDate import scala.collection.JavaConverters._ import scala.util.Try

object FileBasedAvroConsumerJob {

val payloadSchemas: Map[String, Schema] = Map( "NameUpdated" -> new Schema.Parser().parse(Files.newBufferedReader(Paths.get("src/main/avro/NamePayload.avsc"))), "AddressChanged" -> new Schema.Parser().parse(Files.newBufferedReader(Paths.get("src/main/avro/AddressPayload.avsc"))), "IdentificationChanged" -> new Schema.Parser().parse(Files.newBufferedReader(Paths.get("src/main/avro/IdentificationPayload.avsc"))) )

def deserializePayload(payloadBytes: Array[Byte], eventType: String): Option[GenericRecord] = { payloadSchemas.get(eventType).flatMap { schema => val decoder = DecoderFactory.get().binaryDecoder(payloadBytes, null) val reader = new GenericDatumReaderGenericRecord Try(reader.read(null, decoder)).toOption } }

def main(args: Array[String]): Unit = { val spark = SparkSession.builder() .appName("FileBasedAvroConsumerJob") .master("local[*]") .getOrCreate() import spark.implicits._

val baseDirs = Seq("avro_output/existing", "avro_output/new")
val allDirs = baseDirs.flatMap { base =>
  val path = Paths.get(base)
  if (Files.exists(path)) Files.list(path).iterator().asScala.map(_.toString).toSeq else Seq.empty
}

val customerEventSchema = new Schema.Parser().parse(Files.newBufferedReader(Paths.get("src/main/avro/CustomerEvent.avsc")))

val allAvroDF = allDirs.flatMap { path =>
  Try(spark.read.format("avro").load(path)).toOption
}.reduce(_ union _)

val enriched = allAvroDF.flatMap { row =>
  val header = row.getAs[Row]("header")
  val payloadBytes = row.getAs[Array[Byte]]("payload")

  val eventType = header.getAs[String]("event_type")
  val tenantId = header.getAs[Int]("tenant_id")
  val entityId = header.getAs[String]("entity_id")
  val logicalDateInt = header.getAs[Int]("logical_date")
  val eventTimestamp = header.getAs[Long]("event_timestamp")

  val payloadOpt = deserializePayload(payloadBytes, eventType)

  payloadOpt.map { genericRecord =>
    val payloadMap = genericRecord.getSchema.getFields.asScala.map { field =>
      val name = field.name()
      val value = Option(genericRecord.get(name)).map(_.toString).orNull
      name -> value
    }.toMap

    (eventType, tenantId.toString, entityId, logicalDateInt, eventTimestamp, payloadMap)
  }
}.toDF("event_type", "tenant_id", "customer_id", "logical_date_int", "event_timestamp", "payload_map")

val finalDF = enriched
  .withColumn("logical_date", expr("date_add('1970-01-01', logical_date_int)"))
  .withColumn("partition_id", col("customer_id"))
  .select(
    col("event_type"),
    col("tenant_id"),
    col("customer_id"),
    col("logical_date"),
    col("event_timestamp"),
    col("partition_id"),
    col("payload_map")
  )

finalDF.write
  .mode(SaveMode.Overwrite)
  .partitionBy("tenant_id", "partition_id", "logical_date")
  .parquet("parquet_output/events")

spark.stop()

} }

