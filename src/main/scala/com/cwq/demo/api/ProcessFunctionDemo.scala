package com.cwq.demo.api

import com.cwq.demo.window.Sensor
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._


object ProcessFunctionDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    env.enableCheckpointing(60000L)//检查点默认是不开启的，开启检查点设置检查点1秒设置一次
//    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)//设置状态一致性
//    env.getCheckpointConfig.setCheckpointTimeout(20000L)//设置超时时间，检查点20秒没做完的就不要了
//    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)//当设置检查点超时时可能下一个检查点又开始了，设置最大并行度的数量，同时有几个检查点正在工作
//    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000L)//检查点之间的间隔时间
//    env.getCheckpointConfig.setFailOnCheckpointingErrors(true)//当设置检查点时失败了任务要不要失败，默认是true
//    //任务取消时检查点要不要清除
//    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    //从socket中接收的源数据
    val ods: DataStream[String] = env.socketTextStream("localhost",7777)

    //将数据映射成对象
    val obj_ds: DataStream[Sensor] = ods.map(e => {
      val strs: Array[String] = e.split(",")
      Sensor(strs(0), strs(1).toLong, strs(2).toDouble)
    })
    val pf: DataStream[String] = obj_ds.keyBy(_.id).process(new MyProcessFunction())
    pf.print("healthy")
    //侧输出流
    pf.getSideOutput(new OutputTag[String]("warning")).print()

    env.execute("ProcessFunction")
  }
}

/**
  * <I> Type of the input elements.
  * <O> Type of the output elements.
  */
class MyProcessFunction extends ProcessFunction[Sensor,String]{
  override def processElement(value: Sensor, ctx: ProcessFunction[Sensor, String]#Context, out: Collector[String]): Unit = {
    val temperature: Double = value.temperature
    if(temperature<0){
      ctx.output(new OutputTag[String]("warning"),value.id + "低温警报:" + temperature)
    }else{
      out.collect(value.id + "healthy")
    }
  }
}

