package com.cwq.demo.watermaker

import com.cwq.demo.window.Sensor
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.api.scala._
/**
  * 指定字段作为事件时间
  */
object EventTime01 {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)//设置并行度为1
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //使用事件时间
    //获取socket中的数据
    val ds: DataStream[String] = env.socketTextStream("localhost",7777)
    //将输入的字符串映射成对象
    val ds_sensor: DataStream[Sensor] = ds.map(e => {
      val strs: Array[String] = e.split(",")
      Sensor(strs(0), strs(1).toLong, strs(2).toDouble)
    })

    //指定字段作为事件时间
    val event_ds: DataStream[Sensor] = ds_sensor.assignAscendingTimestamps(_.timestamp * 1000L)

    //滚动窗口：窗口大小10s
    val wst: WindowedStream[Sensor, Tuple, TimeWindow] = event_ds.keyBy("id")
      .timeWindow(Time.seconds(10))

    val result: DataStream[Sensor] = wst.reduce((x, y) => { //返回值应该和传入的值的类型保持一样
      val temperature: Double = x.temperature.max(y.temperature)
      y.temperature = temperature
      y
    })
    result.print()
    //执行
    env.execute("tumbling window")
  }
}
