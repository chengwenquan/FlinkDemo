package com.cwq.demo.sink

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

import scala.collection.JavaConversions._
object ElasticSearchSink {
  def main(args: Array[String]): Unit = {
    val httpHosts = List(new HttpHost("hadoop200",9200))
    val es = new ElasticsearchSink.Builder[String](httpHosts,new ElasticsearchSinkFunction[String](){
      override def process(t: String, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        val splits: Array[String] = t.split(",")
        // 用HashMap作为插入es的数据类型 或者可以使用Json,只要是key-value都可以
        val sourceData = new util.HashMap[String, String]()
        sourceData.put("name", splits(0))
        sourceData.put("age", splits(1))
        sourceData.put("length", splits(2))
        // 创建一个index request
        val indexRequest = Requests.indexRequest().index("sensor").`type`("readingData").source(sourceData)
        // 用indexer发送请求
        requestIndexer.add(indexRequest)
      }
    })

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val ds: DataStream[String] = env.readTextFile("D:\\My_pro\\IDEA_Project\\FlinkDemo\\src\\main\\resources\\stu.txt")
    ds.addSink(es.build())
    env.execute("esSink")
  }
}
