package com.cwq.demo.base

import org.apache.flink.api.scala._

/**
  * 批处理
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val inputpath = "D:\\My_pro\\IDEA_Project\\FlinkDemo\\src\\main\\resources\\world.txt"
    val ds: DataSet[String] = env.readTextFile(inputpath)
    ds.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1).print()
  }
}
