package com.cwq.demo.window

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
/**
  * 次数相关的
  * 滚动、滑动窗口测试
  */
object CountWindowDemo {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)//设置并行度为1
    //获取socket中的数据
    val ds: DataStream[String] = env.socketTextStream("localhost",7777)
    //将输入的字符串映射成对象
    val ds_sensor: DataStream[Sensor] = ds.map(new MyMapFunction)

    //5条数据指的是按key分组后组内的数据超过5条就会生成一个新的窗口
    //val wst = ds_sensor.keyBy("id").countWindow(5,)
    //
    val wst = ds_sensor.keyBy("id").countWindow(5,2)

    val result: DataStream[Sensor] = wst.reduce((x, y) => { //返回值应该和传入的值的类型保持一样
      val temperature: Double = x.temperature.max(y.temperature)
      x.temperature = temperature
      x
    })

    result.print()
    env.execute("Count Window")


  }
}

class MyMapFunction() extends MapFunction[String,Sensor]{
  override def map(value: String): Sensor = {
    val strs: Array[String] = value.split(",")
    Sensor(strs(0), strs(1).toLong, strs(2).toDouble)
  }
}