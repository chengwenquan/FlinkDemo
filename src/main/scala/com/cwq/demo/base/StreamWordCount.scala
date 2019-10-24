package com.cwq.demo.base

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
//flatMap和map需要进行隐式转换
import org.apache.flink.api.scala._

/**
  * 流处理
  */
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    //获取传入的参数
    val param: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = param.get("host")
    val port: Int = param.getInt("port")

    //创建env环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //从socket中获取数据
    val ds: DataStream[String] = env.socketTextStream(host,port)
    //对数据进行处理并输出
    ds.flatMap(_.split(" ")).map((_,1)).keyBy(0).sum(1).print()
    //启动executor，并为job命名
    env.execute("StreamWC")
  }

}
