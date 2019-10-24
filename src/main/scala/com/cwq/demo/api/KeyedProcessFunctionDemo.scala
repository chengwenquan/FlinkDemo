package com.cwq.demo.api

import com.cwq.demo.window.Sensor
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
/**
  *需求：监控温度传感器的温度值，如果温度值在一秒钟之内(processing time)连续上升，则报警。
  */
object KeyedProcessFunctionDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //从socket中接收的源数据
    val ods: DataStream[String] = env.socketTextStream("localhost",7777)

    //将数据映射成对象
    val obj_ds: DataStream[Sensor] = ods.map(e => {
      val strs: Array[String] = e.split(",")
      Sensor(strs(0), strs(1).toLong, strs(2).toDouble)
    })
    obj_ds.keyBy(_.id).process(new TempIncreseWarning()).print()
    env.execute("keyedProcessFunction")
  }
}
/**
  * KeyedProcessFunction<K, I, O>
  * <K> Type of the key.
  * <I> Type of the input elements.
  * <O> Type of the output elements.
  */
class TempIncreseWarning() extends KeyedProcessFunction[String,Sensor,String]{

  //记录上次的温度
  lazy val lastTem: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor("temperature",classOf[Double]))
  //上次的时间戳
  lazy val timeStamp: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor("timestamp",classOf[Long]))

  /**
    * 定时时间到之后就会触发该方法
    */
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Sensor, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    out.collect("设备：" + ctx.getCurrentKey + " 温度持续升高 " + timestamp)
    timeStamp.clear()//定时时间到之后清除定时状态
  }
  /**
    * 对分组后组内的数据都会执行
    */
  override def processElement(value: Sensor, ctx: KeyedProcessFunction[String, Sensor, String]#Context,
                              out: Collector[String]): Unit = {
    //获取上次的温度
    val lt: Double = lastTem.value()
    lastTem.update(value.temperature)//更新温度
    //获取时间戳
    val timestamp: Long = timeStamp.value()
    //设置定时器，当温度升高时不进行任何操作，当温度减少时更新定时器
    if( value.temperature>lt && timestamp == 0 ){
      //第一次注册一个定时器
      val timeLong: Long = ctx.timerService().currentProcessingTime() + 10000L //当前时间加10s
      println("定时时间：" + timeLong)
      ctx.timerService().registerProcessingTimeTimer(timeLong)//注册定时器
      timeStamp.update(timeLong)//设置定时状态
    }else{//降温时或恒温时
      ctx.timerService().deleteProcessingTimeTimer(timestamp)//清除定时状态
      timeStamp.clear()//清除定时状态
    }
  }
}