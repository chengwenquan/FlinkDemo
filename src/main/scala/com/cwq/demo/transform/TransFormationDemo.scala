package com.cwq.demo.transform

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

object TransFormationDemo {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val value: DataStream[String] = env.readTextFile("D:\\My_pro\\IDEA_Project\\FlinkDemo\\src\\main\\resources\\stu.txt")

    val ds: DataStream[Student] = value.map(e => {
      val splits: Array[String] = e.split(",")
      Student(splits(0), splits(1).toInt, splits(2).toDouble)
    })
    ds.print("*")

    ds.keyBy("age").reduce((x,y)=>Student(x.name,y.age,y.length))
    env.execute("transform")
  }
}

case class Student(var name:String,var age:Int,var length: Double)
