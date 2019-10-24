package com.cwq.demo.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import scala.util.Random

/**
  * 自定义Source
  */
object CustomSource {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val ds: DataStream[String] = env.addSource(new MyCustomSource())
    ds.print()
    env.execute("customSource")

  }
}

case class MyCustomSource() extends SourceFunction[String]{//String：生成的数据类型

  var running=true
  /**
    * 生成数据的方法
    * @param ctx
    */
  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    val rand = new Random()
    while(running){
      val cm: Long = System.currentTimeMillis()
      var str = "sb_"+rand.nextInt(10)+","+cm+","+rand.nextDouble()*100
      ctx.collect(str)
      Thread.sleep(500)
    }
  }

  /**
    * 停止生成数据的方法
    */
  override def cancel(): Unit = {
    running=false
  }
}
