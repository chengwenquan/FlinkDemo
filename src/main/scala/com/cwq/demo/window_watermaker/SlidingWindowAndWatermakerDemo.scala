package com.cwq.demo.window_watermaker

import com.cwq.demo.window.Sensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * 滑动窗口+水印测试
  */
object TumblingWindowAndWatermakerDemo01 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    //从Socket中接收数据
    val ods: DataStream[String] = env.socketTextStream("localhost",7777)
    //将字符串封装成对象
    val ds_sensor: DataStream[Sensor] = ods.map(e => {
      val strs: Array[String] = e.split(",")
      Sensor(strs(0), strs(1).toLong, strs(2).toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Sensor](Time.seconds(2)) {
      override def extractTimestamp(element: Sensor): Long = element.timestamp
    })
    //输出封装后的数据
    ds_sensor.print("ds_sensor:")
    //分组输出，输出最近10秒内的数据，每5秒输出一次
    ds_sensor.keyBy(_.id)
      .timeWindow(Time.seconds(10),Time.seconds(5))
      .sum(2)
      .print("result:")

    env.execute()
  }
}

// 乱序
// 默认200ms产生一个watermark。数据可延迟2秒
// 自定义每5s开一个新窗口,窗口长度为10s
// 注意：开窗时左闭右开

//sensor_1,1571977520000,1.0  //窗口：[15000-25000] [20000-30000]
//sensor_1,1571977521000,1.0  //窗口：[15000-25000] [20000-30000]
//sensor_1,1571977522000,1.0  //窗口：[15000-25000] [20000-30000]
//sensor_1,1571977523000,1.0  //窗口：[15000-25000] [20000-30000]
//sensor_1,1571977524000,1.0  //窗口：[15000-25000] [20000-30000]
//sensor_1,1571977525000,1.0  //窗口：              [20000-30000] [25000-35000]  水位 23000 [15000-25000]不关闭
//sensor_1,1571977526000,1.0  //窗口：              [20000-30000] [25000-35000]  水位 24000 [15000-25000]不关闭
//sensor_1,1571977527000,1.0  //窗口：              [20000-30000] [25000-35000]  水位 25000 [15000-25000]关闭然后输出结果 result:> Sensor(sensor_1,1571977520000,5.0)
//sensor_1,1571977528000,1.0  //窗口：              [20000-30000] [25000-35000]
//sensor_1,1571977529000,1.0  //窗口：              [20000-30000] [25000-35000]
//sensor_1,1571977530000,1.0  //窗口：                            [25000-35000]
//sensor_1,1571977531000,1.0  //窗口：                            [25000-35000]
//sensor_1,1571977532000,1.0  //窗口：                            [25000-35000] 水位 30000 [20000-30000]关闭然后输出结果 result:> Sensor(sensor_1,1571977520000,10.0)
//sensor_1,1571977533000,1.0  //窗口：                            [25000-35000]
//sensor_1,1571977534000,1.0  //窗口：                            [25000-35000]
//sensor_1,1571977535000,1.0  //窗口：                                          [35000-45000]
//sensor_1,1571977536000,1.0  //窗口：                                          [35000-45000]

