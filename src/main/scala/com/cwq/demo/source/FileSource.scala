package com.cwq.demo.source

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object FileSource {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val ds: DataStream[String] = env.readTextFile("D:\\My_pro\\IDEA_Project\\FlinkDemo\\src\\main\\resources\\SensorReading.txt")
    ds.print()
    env.execute("FileSource")
  }
}
