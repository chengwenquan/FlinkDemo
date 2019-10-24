package com.cwq.demo.source

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * 从集合读取数据
  */
object CollectionSource {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val ds: DataStream[SensorReading] = env.fromCollection(List(SensorReading("sb_1001", 1547718196, 32.96),
      SensorReading("sb_1002", 1547718197, 42.97), SensorReading("sb_1003", 1547718198, 52.98),
      SensorReading("sb_1004", 1547718199, 62.99), SensorReading("sb_1005", 1547718200, 72.00)))
    ds.print("haha").setParallelism(2)
    env.execute("sensor")
  }
}

// 定义样例类，传感器id，时间戳，温度
case class SensorReading(id: String, timestamp: Long, temperature: Double)
