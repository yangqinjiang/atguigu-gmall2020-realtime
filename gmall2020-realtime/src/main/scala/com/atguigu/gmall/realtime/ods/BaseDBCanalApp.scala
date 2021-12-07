package com.atguigu.gmall.realtime.ods

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.atguigu.gmall.realtime.utils.{MyKafkaSink, MyKafkaUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

//基于Canal从Kafka中读取业务数据, 进行分流
object BaseDBCanalApp {

  def main(args: Array[String]): Unit = {
    var sparkConf = new SparkConf().setMaster("local[4]").setAppName("BaseDBCanalApp").set("spark.testing.memory", "2147480000")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val topic = "gmall2020_db_c"
    val groupId = "base_db_canal_group"
    //定义一个可变的变量 var
    var recoredDStream: InputDStream[ConsumerRecord[String, String]] = null
    //从Redis中读取偏移量
    val kafkaOffsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    if(null != kafkaOffsetMap && kafkaOffsetMap.nonEmpty){
      recoredDStream = MyKafkaUtil.getKafkaStream(topic, ssc, kafkaOffsetMap, groupId)
    }else{
      recoredDStream = MyKafkaUtil.getKafkaStream(topic,ssc, groupId)
    }

    //获取当前 采集周期中处理的数据, 对应的分区的偏移量
    var offsetRanges:Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recoredDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

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

    //启动
    ssc.start()
    ssc.awaitTermination()
  }
}
