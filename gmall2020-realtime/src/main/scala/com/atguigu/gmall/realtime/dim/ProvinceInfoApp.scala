package com.atguigu.gmall.realtime.dim

import com.atguigu.gmall.realtime.bean.ProvinceInfo
import com.atguigu.gmall.realtime.common.{RTApp, StartConf}
import com.atguigu.gmall.realtime.config.ApplicationConfig
import com.atguigu.gmall.realtime.utils.OffsetManagerUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

/**
 * 从kafka中读取省份维度数据,写入到Hbase中
 */
object ProvinceInfoApp extends App with RTApp with Logging {

  val conf = StartConf("ods_base_province", "gmall_province_info_group")

  //启动应用程序
  start(conf) {
    (offsetDStream: DStream[ConsumerRecord[String, String]],
     topic: String, groupId: String) => {
      //转换结构
      import com.atguigu.gmall.realtime.utils.MyImplicit.transformToObj
      val objectDStream: DStream[ProvinceInfo] = offsetDStream

      import org.apache.phoenix.spark._
      objectDStream.foreachRDD {
        rdd: RDD[ProvinceInfo] => {
          logInfo("saveToPhoenix start....")
          //保存到hbase
          rdd.saveToPhoenix(
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
