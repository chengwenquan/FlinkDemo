package com.cwq.demo.sink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

object KafkaSink {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val ds: DataStream[String] = env.readTextFile("D:\\My_pro\\IDEA_Project\\FlinkDemo\\src\\main\\resources\\stu.txt")
    ds.addSink(new FlinkKafkaProducer011[String]("hadoop200:9092","flink01",new SimpleStringSchema()))
    env.execute("kafkaSink")




  }
}
