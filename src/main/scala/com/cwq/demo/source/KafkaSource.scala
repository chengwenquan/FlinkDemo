package com.cwq.demo.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.api.scala._

object KafkaSource {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties
    properties.setProperty("bootstrap.servers", "hadoop200:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    //SimpleStringSchema 将 kafka 获取的字节流转换为字符串。
    val kafkads: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("flinktest",new SimpleStringSchema,properties))
    kafkads.print()
    env.execute("kafkaSource")
  }
}
