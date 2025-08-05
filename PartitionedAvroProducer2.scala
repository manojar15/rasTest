package rastest

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.util.Random
import scala.io.Source

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

object ParallelPartitionedAvroEventFileProducer {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Parallel Partitioned Avro File Producer")
      .master("local[8]")
      .config("spark.sql.shuffle.partitions", "64")
      .getOrCreate()

    import spark.implicits._

    val tenantId = 42
    val logicalDate = LocalDate.parse("2025-04-03", DateTimeFormatter.ISO_DATE)
    val logicalDateDays = logicalDate.toEpochDay.toInt
    val eventTypes = Seq("NAME", "ADDRESS", "IDENTIFICATION")
    val outputPath = "customer/event_output"

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

    // ----------- Existing customers: 4M events (random event types) -----------
    val existingEventsRDD = spark.sparkContext.parallelize(1 to 4000000, 64).mapPartitions { part =>
      val rnd = new Random()
      val customers = broadcastCustomerIds.value

      part.map { _ =>
        val customerId = customers(rnd.nextInt(customers.size))
        val eventType = eventTypes(rnd.nextInt(eventTypes.size))

        val header = new GenericData.Record(headerSchema)
        header.put("event_timestamp", System.currentTimeMillis())
        header.put("logical_date", logicalDateDays)
        header.put("event_type", eventType)
        header.put("tenant_id", tenantId)
        header.put("entity_id", customerId)

        val (schema, payloadRecord) = eventType match {
          case "NAME" =>
            val rec = new GenericData.Record(nameSchema)
            rec.put("customer_id", customerId)
            rec.put("first", s"UpdatedFirst_$customerId")
            rec.put("middle", "Z")
            rec.put("last", s"UpdatedLast_$customerId")
            (nameSchema, rec)
          case "ADDRESS" =>
            val rec = new GenericData.Record(addressSchema)
            rec.put("customer_id", customerId)
            val addrType = if (rnd.nextBoolean()) "HOME" else "WORK"
            rec.put("type", new GenericData.EnumSymbol(addressSchema.getField("type").schema(), addrType))
            rec.put("street", s"NewStreet $customerId")
            rec.put("city", s"NewCity_$customerId")
            rec.put("postal_code", f"NewPC_${rnd.nextInt(999)}%03d")
            rec.put("country", "NewCountryX")
            (addressSchema, rec)
          case "IDENTIFICATION" =>
            val rec = new GenericData.Record(idSchema)
            rec.put("customer_id", customerId)
            rec.put("type", "passport")
            rec.put("number", s"NEWID_$customerId")
            rec.put("issuer", "GovX")
            (idSchema, rec)
        }

        val payloadBaos = new ByteArrayOutputStream()
        val payloadEncoder = EncoderFactory.get().binaryEncoder(payloadBaos, null)
        val payloadWriter = new GenericDatumWriter[GenericRecord](schema)
        payloadWriter.write(payloadRecord, payloadEncoder)
        payloadEncoder.flush()
        payloadBaos.close()

        val wrapperRecord = new GenericData.Record(wrapperSchema)
        wrapperRecord.put("header", header)
        wrapperRecord.put("payload", ByteBuffer.wrap(payloadBaos.toByteArray))

        wrapperRecord // <<<< Only the Avro record! Not flattened
      }
    }

    // ----------- New customers: 500K, 5 events each (like original job) -----------
    val newCustomersRDD = spark.sparkContext.parallelize(1 to 500000, 32).flatMap { _ =>
      val rnd = new Random()
      val partitionId = rnd.nextInt(8)
      val customerId = f"${partitionId}_9${rnd.nextInt(999999)}%06d"

      Seq(
        ("NAME", {
          val rec = new GenericData.Record(nameSchema)
          rec.put("customer_id", customerId)
          rec.put("first", s"NewFirst_$customerId")
          rec.put("middle", "M")
          rec.put("last", s"NewLast_$customerId")
          rec
        }),
        ("ADDRESS", {
          val rec = new GenericData.Record(addressSchema)
          rec.put("customer_id", customerId)
          rec.put("type", new GenericData.EnumSymbol(addressSchema.getField("type").schema(), "HOME"))
          rec.put("street", s"Street $customerId")
          rec.put("city", s"City_$partitionId")
          rec.put("postal_code", f"PC_${rnd.nextInt(9999)}%04d")
          rec.put("country", "CountryY")
          rec
        }),
        ("ADDRESS", {
          val rec = new GenericData.Record(addressSchema)
          rec.put("customer_id", customerId)
          rec.put("type", new GenericData.EnumSymbol(addressSchema.getField("type").schema(), "WORK"))
          rec.put("street", s"WorkStreet $customerId")
          rec.put("city", s"WorkCity_$partitionId")
          rec.put("postal_code", f"WPC_${rnd.nextInt(9999)}%04d")
          rec.put("country", "CountryZ")
          rec
        }),
        ("IDENTIFICATION", {
          val rec = new GenericData.Record(idSchema)
          rec.put("customer_id", customerId)
          rec.put("type", "passport")
          rec.put("number", s"ID1_$customerId")
          rec.put("issuer", "Issuer1")
          rec
        }),
        ("IDENTIFICATION", {
          val rec = new GenericData.Record(idSchema)
          rec.put("customer_id", customerId)
          rec.put("type", "driver_license")
          rec.put("number", s"ID2_$customerId")
          rec.put("issuer", "Issuer2")
          rec
        })
      ).map { case (eventType, payloadRecord) =>
        val header = new GenericData.Record(headerSchema)
        header.put("event_timestamp", System.currentTimeMillis())
        header.put("logical_date", logicalDateDays)
        header.put("event_type", eventType)
        header.put("tenant_id", tenantId)
        header.put("entity_id", customerId)
        val payloadBaos = new ByteArrayOutputStream()
        val payloadEncoder = EncoderFactory.get().binaryEncoder(payloadBaos, null)
        val payloadWriter = new GenericDatumWriter[GenericRecord](payloadRecord.getSchema)
        payloadWriter.write(payloadRecord, payloadEncoder)
        payloadEncoder.flush()
        payloadBaos.close()
        val wrapperRecord = new GenericData.Record(wrapperSchema)
        wrapperRecord.put("header", header)
        wrapperRecord.put("payload", ByteBuffer.wrap(payloadBaos.toByteArray))
        wrapperRecord
      }
    }

    // Union all events (existing + new)
    val allEventsRDD = existingEventsRDD.union(newCustomersRDD)

    // Save all as Avro files
    spark.createDataset(allEventsRDD)(org.apache.spark.sql.Encoders.kryo[GenericRecord])
      .write
      .format("avro")
      .option("avroSchema", wrapperSchemaStr)
      .save(outputPath)

    println("✅ Finished producing and saving full Avro event records for downstream consumer flattening.")
    spark.stop()
  }
}
