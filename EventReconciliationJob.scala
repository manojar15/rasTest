package rastest

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.util.Properties
import scala.jdk.CollectionConverters._

object EventReconciliationJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Event Reconciliation Job")
      .master("local[*]")
      .getOrCreate()

    val kafkaBootstrap = "localhost:9092"
    val topic = "customer_events_partitioned_v2"
    //val logicalDate = args.headOption.getOrElse(LocalDate.now().toString)
    val logicalDate = "2025-04-03"
    val ingestedPath = s"file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/tenant_data/tenant_id=42/partition_id=*/logical_date=$logicalDate"

    val df = spark.read.format("parquet").load(ingestedPath)
    val ingestedCount = df.count()

    println(s"✅ Ingested record count for $logicalDate: $ingestedCount")

    // Connect to Kafka and get the count of messages by reading offsets
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap)
    val admin = AdminClient.create(props)

    val topicDesc = admin.describeTopics(List(topic).asJava).all().get()
    val partitions = topicDesc.get(topic).partitions()

    val consumerGroup = "event_consumer_group"
    val consumerOffsets = admin.listConsumerGroupOffsets(consumerGroup).partitionsToOffsetAndMetadata().get()

    val kafkaCount = partitions.stream().mapToLong { partitionInfo =>
      val tp = new org.apache.kafka.common.TopicPartition(topic, partitionInfo.partition())
      val offsetMeta = consumerOffsets.get(tp)
      if (offsetMeta != null) offsetMeta.offset() else 0L
    }.sum()

    println(s"📦 Kafka consumed offset count for group '$consumerGroup': $kafkaCount")
    println(s"🔍 Reconciliation delta: ${kafkaCount - ingestedCount}")

    admin.close()
    spark.stop()
  }
}

