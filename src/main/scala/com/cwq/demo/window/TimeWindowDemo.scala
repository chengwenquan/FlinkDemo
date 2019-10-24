package com.cwq.demo.window

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.api.scala._

/**
  * 时间相关的
  * 滚动\滑动窗口测试
  */
object TimeWindowDemo {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)//设置并行度为1
    //获取socket中的数据
    val ds: DataStream[String] = env.socketTextStream("localhost",7777)
    //将输入的字符串映射成对象
    val ds_sensor: DataStream[Sensor] = ds.map(e => {
      val strs: Array[String] = e.split(",")
      Sensor(strs(0), strs(1).toLong, strs(2).toDouble)
    })
    //滚动窗口：窗口大小10s
    val wst: WindowedStream[Sensor, Tuple, TimeWindow] = ds_sensor.keyBy("id")
      .timeWindow(Time.seconds(10))
//    //滑动窗口：窗口大小10s、步长5s
//    val wst: WindowedStream[Sensor, Tuple, TimeWindow] = ds_sensor.keyBy("id")
//      .timeWindow(Time.seconds(10),Time.seconds(5))

    val result: DataStream[Sensor] = wst.reduce((x, y) => { //返回值应该和传入的值的类型保持一样
      val temperature: Double = x.temperature.max(y.temperature)
      x.temperature = temperature
      x
    })
    result.print()
    //执行
    env.execute("tumbling window")
  }
}

//id设备 timestamp时间戳 temperature温度
case class Sensor(var id:String,var timestamp:Long,var temperature:Double)

