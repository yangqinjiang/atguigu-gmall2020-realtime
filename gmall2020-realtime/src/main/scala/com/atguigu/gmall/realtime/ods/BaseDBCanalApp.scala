package com.atguigu.gmall.realtime.ods

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.atguigu.gmall.realtime.common.{RTApp, StartConf}
import com.atguigu.gmall.realtime.utils.{MyKafkaSink, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.DStream

//基于Canal从Kafka中读取业务数据, 进行分流
object BaseDBCanalApp extends App with RTApp {
  val conf = StartConf("gmall2020_db_c", "base_db_canal_group")

  //启动应用程序
  start(conf) {
    (offsetDStream: DStream[ConsumerRecord[String, String]],
     topic: String, groupId: String) => {

      //将从kafka中读取到的record数据进行封装为json对象
      val jsonObjDStream: DStream[JSONObject] = offsetDStream.map {
        record: ConsumerRecord[String, String] => {
          //获取value部分的json字符串
          val jsonStr: String = record.value()
          //将json格式字符串转换为json对象
          val jsonObject: JSONObject = JSON.parseObject(jsonStr)
          jsonObject
        }
      }

      //从json对象中获取table和data, 发送到不同的kafka主题
      //foreachRDD是行动算子
      val res: Unit = jsonObjDStream.foreachRDD {
        rdd => {
          rdd.foreach {
            jsonObj => {
              //获取更新的表名
              val tableName: String = jsonObj.getString("table")
              //获取当前对表数据的更新
              val dataArr: JSONArray = jsonObj.getJSONArray("data")
              val opType: String = jsonObj.getString("type")
              //拼接发送的主题,ods 原始数据层
              var sendTopic = "ods_" + tableName
              import scala.collection.JavaConverters._
              //比较字符串, 统一使用大写的字符 toUpperCase
              if ("INSERT".equals(opType.toUpperCase())) {
                for (data <- dataArr.asScala) {
                  val msg: String = data.toString
                  //向kafka发送信息
                  MyKafkaSink.send(sendTopic, msg) //注意,不要向 val topic = "gmall2020_db_c" 发送
                }
              }
            }
          }
          //当前批次结束, 保存偏移量
          OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
        }
      }
    }
  }
}
