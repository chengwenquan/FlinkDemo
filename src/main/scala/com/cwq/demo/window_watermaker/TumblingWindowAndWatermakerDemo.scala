package com.cwq.demo.window_watermaker

import com.cwq.demo.window.Sensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._

/**
  * 滚动窗口 + 水印测试
  */
object TumblingWindowAndWatermakerDemo001 {
  //有序
  //默认200ms产生一个watermark。
  // 自定义每10秒一个窗口，开窗时左闭右开
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val ods: DataStream[String] = env.socketTextStream("localhost",7777)

    val ds_sensor: DataStream[Sensor] = ods.map(e => {
      val strs: Array[String] = e.split(",")
      Sensor(strs(0), strs(1).toLong, strs(2).toDouble)
    }).assignAscendingTimestamps(_.timestamp) //指定时间戳
    ds_sensor.print("ds_sensor:")
    ds_sensor.keyBy(_.id)
      .timeWindow(Time.seconds(10))
      .sum(2)
      .print("result:")

    env.execute()
  }
}


object TumblingWindowAndWatermakerDemo002 {
  // 乱序
  // 默认200ms产生一个watermark延时2秒
  // 自定义每10秒一个窗口，开窗时左闭右开
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val ods: DataStream[String] = env.socketTextStream("localhost",7777)

    val ds_sensor: DataStream[Sensor] = ods.map(e => {
      val strs: Array[String] = e.split(",")
      Sensor(strs(0), strs(1).toLong, strs(2).toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Sensor](Time.seconds(2)) {
      override def extractTimestamp(element: Sensor): Long = element.timestamp
    })
    ds_sensor.print("ds_sensor:")
    ds_sensor.keyBy(_.id)
      .timeWindow(Time.seconds(10))
      .sum(2)
      .print("result:")

    env.execute()
  }
}

//nc输入如下数据
//sensor_1,1571977520000,1.1
//sensor_1,1571977525000,2.1
//sensor_1,1571977530000,3.1
//sensor_1,1571977529000,2.0
//sensor_1,1571977523000,2.0
//sensor_1,1571977510000,2.0
//sensor_1,1571977532000,4.1
//控制台输出结果
//ds_sensor:> Sensor(sensor_1,1571977520000,1.1)  //窗口：20000-30000，水位：1571977518000，操作：窗口10000-20000还在
//ds_sensor:> Sensor(sensor_1,1571977525000,2.1)  //窗口20000-30000，水位：1571977523000，操作：窗口10000-20000关闭因为没有数据所以没有输出数据
//ds_sensor:> Sensor(sensor_1,1571977530000,3.1)  //窗口30000-40000，水位：1571977528000，操作：本该把窗口20000-30000关闭的但是延时了2秒所以不会关闭
//ds_sensor:> Sensor(sensor_1,1571977529000,2.0)  //窗口20000-30000，水位：1571977528000
//ds_sensor:> Sensor(sensor_1,1571977523000,2.0)  //窗口20000-30000，水位：1571977528000
//ds_sensor:> Sensor(sensor_1,1571977510000,2.0)  //窗口10000-20000已关闭，写不进去
//ds_sensor:> Sensor(sensor_1,1571977532000,4.1)  //窗口30000-40000，水位：1571977530000，操作：把窗口20000-30000关闭，会输出结果
//result:> Sensor(sensor_1,1571977520000,7.2)

//小于20000的数据进入10000-20000窗口 在10000<= n <22000时可以向窗口写数据
// 时间为22000时水印为20000，窗口关闭不能向窗口中写数据，关闭的窗口只在关闭时计算一次
//大于20000的数据进入20000-30000窗口 时间为32000时水印为30000，窗口关闭不能向窗口中写数据


object TumblingWindowAndWatermakerDemo003 {
  // 乱序
  // 默认200ms产生一个watermark延时2秒，
  // 自定义每10秒一个窗口，窗口延迟5秒关闭，开窗时左闭右开
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val ods: DataStream[String] = env.socketTextStream("localhost",7777)

    val ds_sensor: DataStream[Sensor] = ods.map(e => {
      val strs: Array[String] = e.split(",")
      Sensor(strs(0), strs(1).toLong, strs(2).toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Sensor](Time.seconds(2)) {
      override def extractTimestamp(element: Sensor): Long = element.timestamp
    })
    ds_sensor.print("ds_sensor:")
    ds_sensor.keyBy(_.id)
      .timeWindow(Time.seconds(10))
      .allowedLateness(Time.seconds(5))
      .sum(2)
      .print("result:")

    env.execute()
  }
}
//nc输入如下数据
//sensor_1,1571977520000,1.1  //窗口20000-30000
//sensor_1,1571977525000,2.1  //窗口20000-30000
//sensor_1,1571977530000,3.1  //窗口20000-30000
//sensor_1,1571977532000,2.0  //窗口30000-40000，窗口20000-30000应该关闭但设置了延迟5秒关闭不会真的关闭，只是统计了一些然后输出结果 sensor_1,1571977520000,3.2（到达关闭时间后立即会输出结果，但并不关闭，如果此后5秒内又有数据那么依然可以进入，此刻只会更新原有结果并输出。如果超过了5秒就再也不能进入窗口了）
//sensor_1,1571977535000,2.0  //窗口30000-40000
//sensor_1,1571977528000,2.0  //窗口20000-30000 能进入并会更新结果并输出sensor_1,1571977520000,5.2
//sensor_1,1571977524000,2.0  //窗口20000-30000 能进入并会更新结果并输出sensor_1,1571977520000,7.2
//sensor_1,1571977537000,2.0  //窗口30000-40000，窗口20000-30000要关闭，并不会再次输出20000-30000的结果
//sensor_1,1571977529000,2.0  //窗口20000-30000已经关闭，不能再进入了
//控制台输出结果
//ds_sensor:> Sensor(sensor_1,1571977520000,1.1)
//ds_sensor:> Sensor(sensor_1,1571977525000,2.1)
//ds_sensor:> Sensor(sensor_1,1571977530000,3.1)
//ds_sensor:> Sensor(sensor_1,1571977532000,2.0)
//result:> Sensor(sensor_1,1571977520000,3.2)
//ds_sensor:> Sensor(sensor_1,1571977535000,2.0)
//ds_sensor:> Sensor(sensor_1,1571977528000,2.0)
//result:> Sensor(sensor_1,1571977520000,5.2)
//ds_sensor:> Sensor(sensor_1,1571977524000,2.0)
//result:> Sensor(sensor_1,1571977520000,7.2)
//ds_sensor:> Sensor(sensor_1,1571977537000,2.0)
//ds_sensor:> Sensor(sensor_1,1571977529000,2.0)