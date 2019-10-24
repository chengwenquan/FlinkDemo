package com.cwq.demo.tableapi

import com.cwq.demo.window.Sensor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
//隐式转换
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.scala._
object TableDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val ds: DataStream[String] = env.socketTextStream("localhost",7777)
    val stable_env: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)

    val obj_ds: DataStream[Sensor] = ds.map(e => {
      val strs: Array[String] = e.split(",")
      Sensor(strs(0), strs(1).toLong, strs(2).toDouble)
    })
    val table: Table = stable_env.fromDataStream(obj_ds)
    table.select("id,timestamp").toAppendStream[(String,Long)].print()
    env.execute()
  }
}


