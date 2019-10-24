package com.cwq.demo

object TestDemo {

  def main(args: Array[String]): Unit = {
    var str = "zhangsan,19,178"
    val strings: Array[String] = str.split(",")

    println(strings(2))
  }

}
