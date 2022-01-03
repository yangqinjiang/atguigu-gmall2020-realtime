package com.atguigu.gmall.realtime.dim

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.realtime.bean.ProvinceInfo
import com.atguigu.gmall.realtime.common.{RTApp, StartConf}
import com.atguigu.gmall.realtime.config.ApplicationConfig
import com.atguigu.gmall.realtime.utils.OffsetManagerUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

/**
 * 从kafka中读取省份维度数据,写入到Hbase中
 */
object ProvinceInfoApp extends App with RTApp with Logging{

  val conf = StartConf("local[3]",
    "ods_base_province", "gmall_province_info_group", Seconds(5))

  //启动应用程序
  start(conf) {
    (offsetDStream: DStream[ConsumerRecord[String, String]],
     topic: String, groupId: String) => {
      //写入到hbase中
      offsetDStream.foreachRDD {
        rdd: RDD[ConsumerRecord[String, String]] => {
          val provinceInfoRDD: RDD[ProvinceInfo] = rdd.map {
            record: ConsumerRecord[String, String] => {
              //得到从kafka中读取的jsonString
              val jsonString: String = record.value()
              //转换为ProvinceInfo样例类
              val provinceInfo: ProvinceInfo = JSON.parseObject(jsonString, classOf[ProvinceInfo])
              provinceInfo
            }
          }

          logInfo("saveToPhoenix start....")
          //保存到hbase
          import org.apache.phoenix.spark._
          provinceInfoRDD.saveToPhoenix(
            "gmall2020_province_info",
            Seq("ID", "NAME", "AREA_CODE", "ISO_CODE"),
            new Configuration,
            Some(ApplicationConfig.HBASE_HOST)
          )
          logInfo("saveToPhoenix finish....")
          //处理完数据, 再保存偏移量
          OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
        }
      }
    }
  }
}
