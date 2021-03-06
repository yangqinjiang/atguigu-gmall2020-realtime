package com.atguigu.gmall.realtime.dim

import com.atguigu.gmall.realtime.bean.SpuInfo
import com.atguigu.gmall.realtime.common.{RTApp, StartConf}
import com.atguigu.gmall.realtime.config.ApplicationConfig
import com.atguigu.gmall.realtime.utils.OffsetManagerUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.DStream

/**
 * 读取商品spu维度数据到Hbase
 */
object SpuInfoApp extends App with RTApp {
  val conf = StartConf("ods_spu_info", "gmall_spu_info_group")

  //启动应用程序
  start(conf) {
    (offsetDStream: DStream[ConsumerRecord[String, String]],
     topic: String, groupId: String) => {

      //转换结构
      import com.atguigu.gmall.realtime.utils.MyImplicit.transformToObj
      val objectDStream: DStream[SpuInfo] = offsetDStream

      //保存到hbase
      import org.apache.phoenix.spark._
      objectDStream.foreachRDD {
        rdd => {
          rdd.saveToPhoenix(
            "gmall2020_spu_info",
            Seq("ID", "SPU_NAME"),
            new Configuration,
            Some(ApplicationConfig.HBASE_HOST)
          )
          //处理完数据, 再保存偏移量
          OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
        }
      }
    }
  }
}
