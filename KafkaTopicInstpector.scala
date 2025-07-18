package rastest

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, ListConsumerGroupOffsetsOptions}
import java.util.Properties
import scala.jdk.CollectionConverters._

object KafkaTopicInspector {
  def main(args: Array[String]): Unit = {
    val bootstrapServers = "localhost:9092"
    val topic = "customer_events_partitioned_v2"

    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)

    val adminClient = AdminClient.create(props)

    try {
      val topicDescription = adminClient.describeTopics(List(topic).asJava).all().get()

      println(s"\n✅ Topic '$topic' metadata:")
      val partitions = topicDescription.get(topic).partitions().asScala
      partitions.foreach { p =>
        println(s"Partition: ${p.partition()}, Leader: ${p.leader()}, Replicas: ${p.replicas().asScala.mkString(",")}")
      }

      val offsetSpecs = partitions.map(p =>
        new org.apache.kafka.common.TopicPartition(topic, p.partition()) -> org.apache.kafka.clients.admin.OffsetSpec.latest()
      ).toMap.asJava

      val offsets = adminClient.listOffsets(offsetSpecs).all().get()

      println("\nCurrent Offsets:")
      offsets.asScala.foreach { case (tp, offset) =>
        println(s"Partition ${tp.partition()}: ${offset.offset()}")
      }

      val groups = adminClient.listConsumerGroups().all().get().asScala
      println("\nConsumer Groups:")
      groups.foreach { group =>
        println(s"- Group ID: ${group.groupId()}")

        val groupOffsets = adminClient.listConsumerGroupOffsets(group.groupId(), new ListConsumerGroupOffsetsOptions()).partitionsToOffsetAndMetadata().get()
        groupOffsets.asScala.foreach { case (tp, offsetMeta) =>
          if (tp.topic() == topic) {
            println(s"    Partition ${tp.partition()}: Offset ${offsetMeta.offset()}")
          }
        }
      }

    } finally {
      adminClient.close()
    }
  }
}