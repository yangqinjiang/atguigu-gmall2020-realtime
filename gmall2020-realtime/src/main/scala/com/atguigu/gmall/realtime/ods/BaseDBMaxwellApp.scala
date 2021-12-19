package com.atguigu.gmall.realtime.ods

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.common.{RTApp, StartConf}
import com.atguigu.gmall.realtime.utils.{MyKafkaSink, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

//基于Maxwell从Kafka中读取业务数据, 进行分流
object BaseDBMaxwellApp  extends App with RTApp {
  val conf = StartConf("local[3]",
    "gmall2020_db_m", "base_db_maxwell_group", Seconds(5))

  //启动应用程序
  start(conf){
    (offsetDStream: DStream[ConsumerRecord[String, String]],
     topic: String, groupId: String) =>{

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
      //只考虑需要的数据表
      def tableAndOp(opType: String, tableName: String):Boolean = {
        if(("order_info".equals(tableName) && "insert".equals(opType)) // 订单表,新增数据insert
          ||("order_detail".equals(tableName) && "insert".equals(opType)) //订单明细表,新增数据insert
          || "base_province".equals(tableName) /* 省份表,insert,update等等*/
          || "user_info".equals(tableName) /* 用户表,insert,update等等*/
          || "sku_info".equals(tableName) /* sku表,insert,update等等*/
          || "base_trademark".equals(tableName) /* base_trademark表,insert,update等等*/
          || "base_category3".equals(tableName) /* base_category3表,insert,update等等*/
          || "spu_info".equals(tableName) /* spu_info表,insert,update等等*/
        ) true else false
      }
      //从json对象中获取table和data, 发送到不同的kafka主题
      //foreachRDD是行动算子
      val res: Unit = jsonObjDStream.foreachRDD {
        rdd: RDD[JSONObject] => {
          rdd.foreach {
            jsonObj: JSONObject => {
              val opType: String = jsonObj.getString("type")
              //获取当前对表数据的更新
              val dataJsonObj: JSONObject = jsonObj.getJSONObject("data")
              //获取更新的表名
              val tableName: String = jsonObj.getString("table")
              //            import scala.collection.JavaConverters._
              //比较字符串, 统一使用大写的字符 toUpperCase
              if (dataJsonObj != null && !dataJsonObj.isEmpty && tableName != null && opType != null) {
                //只考虑需要的数据表
                if (tableAndOp(opType, tableName)) {
                  //拼接发送的主题,ods 原始数据层
                  val sendTopic = "ods_" + tableName
                  //向kafka发送信息
                  MyKafkaSink.send(sendTopic, dataJsonObj.toString) //注意,不要向 val topic = "gmall2020_db_c" 发送
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
