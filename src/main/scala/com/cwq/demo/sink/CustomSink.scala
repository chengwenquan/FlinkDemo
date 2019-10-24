package com.cwq.demo.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object CustomSink {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val ds: DataStream[String] = env.readTextFile("D:\\My_pro\\IDEA_Project\\FlinkDemo\\src\\main\\resources\\stu.txt")
    ds.addSink(new MyJdbcSink())
    env.execute("customSink")
  }

}

class MyJdbcSink() extends RichSinkFunction[String]{
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  //调用连接，执行sql
  override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {
    val str: Array[String] = value.split(",")
    updateStmt.setString(1, str(1))
    updateStmt.setString(2, str(2))
    updateStmt.setString(3, str(0))
    updateStmt.execute()

    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setString(1, str(0))
      insertStmt.setString(2, str(1))
      insertStmt.setString(3, str(2))
      insertStmt.execute()
    }
  }
  // open 主要是创建连接
  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://hadoop200:3306/test", "cwq", "123456")
    insertStmt = conn.prepareStatement("INSERT INTO stu (name, age,length) VALUES (?,?,?)")
    updateStmt = conn.prepareStatement("UPDATE stu SET age = ?,length=? WHERE name = ?")
  }

  // 关闭链接
  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}