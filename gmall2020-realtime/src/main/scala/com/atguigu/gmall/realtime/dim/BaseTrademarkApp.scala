package com.atguigu.gmall.realtime.dim

import com.atguigu.gmall.realtime.bean.BaseTrademark
import com.atguigu.gmall.realtime.common.{RTApp, StartConf}
import com.atguigu.gmall.realtime.config.ApplicationConfig
import com.atguigu.gmall.realtime.utils.OffsetManagerUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.language.implicitConversions

/**
 * 读取商品品牌维度数据到Hbase
 */
object BaseTrademarkApp extends App with RTApp {
  val conf = StartConf("ods_base_trademark", "gmall_base_trademark_group")

  //启动应用程序
  start(conf) {
    (offsetDStream: DStream[ConsumerRecord[String, String]],
     topic: String, groupId: String) => {
     //隐式转换
      import com.atguigu.gmall.realtime.utils.MyImplicit.transformToObj
      val objectDStream:DStream[BaseTrademark]  = offsetDStream

      //保存到hbase
      import org.apache.phoenix.spark._
      objectDStream.foreachRDD {
        rdd: RDD[BaseTrademark] => {
          rdd.saveToPhoenix(
            "gmall2020_base_trademark",
            Seq("ID", "TM_NAME"),
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
