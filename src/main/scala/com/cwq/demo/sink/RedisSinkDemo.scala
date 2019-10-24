package com.cwq.demo.sink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}



object RedisSinkDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val ds: DataStream[String] = env.readTextFile("D:\\My_pro\\IDEA_Project\\FlinkDemo\\src\\main\\resources\\stu.txt")
    val conf = new FlinkJedisPoolConfig.Builder().setHost("hadoop202").setPort(6379).build()
    ds.addSink(new RedisSink[String](conf,new MyRedisMapper()))
    env.execute("Redis Sink Demo")
  }
}

class MyRedisMapper extends RedisMapper[String]{
  //要执行的命令
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.SET)
  }
  //指定key
  override def getKeyFromData(data: String): String = {
    val strings: Array[String] = data.split(",")
    strings(0)
  }
  //指定value
  override def getValueFromData(data: String): String = {
   data
  }
}