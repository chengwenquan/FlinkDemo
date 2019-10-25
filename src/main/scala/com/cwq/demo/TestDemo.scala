package com.cwq.demo

import java.util.Date


object TestDemo {

  def main(args: Array[String]): Unit = {


    val date = new Date()
    val time: Long = date.getTime
    println(time)
  }

}
