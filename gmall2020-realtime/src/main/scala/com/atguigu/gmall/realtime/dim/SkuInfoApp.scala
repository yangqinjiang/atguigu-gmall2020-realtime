package com.atguigu.gmall.realtime.dim

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.bean.{BaseCategory3, BaseTrademark, SkuInfo, SpuInfo}
import com.atguigu.gmall.realtime.common.{RTApp, StartConf}
import com.atguigu.gmall.realtime.config.ApplicationConfig
import com.atguigu.gmall.realtime.utils.{OffsetManagerUtil, PhoenixUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

/**
 * 读取商品维度数据,并关联品牌,分类,spu, 保存到Hbase
 */
object SkuInfoApp extends App with RTApp {
  val conf = StartConf("local[3]",
    "ods_sku_info", "gmall_sku_info_group", Seconds(5))

  //启动应用程序
  start(conf) {
    (offsetDStream: DStream[ConsumerRecord[String, String]],
     topic: String, groupId: String) => {

      //转换结构
      val objectDStream: DStream[SkuInfo] = offsetDStream.map {
        record => {
          val jsonStr: String = record.value()
          val obj: SkuInfo = JSON.parseObject(jsonStr, classOf[SkuInfo])
          obj
        }
      }

      //TODO: 商品和品牌,分类,Spu维度表进行关联, 这是退化维度, 只是为了方便订单明细与维度表关联时, 高效些
      //
      val skuInfoDStream: DStream[SkuInfo] = objectDStream.transform {
        rdd: RDD[SkuInfo] => {
          //        rdd.cache()
          //        val c0 = rdd.count()
          ////        println("count0=", c0)
          //        //这些代码在driver端运行
          //        if (c0 <= 0) {
          //          rdd
          //        } else {
          //rdd 不为空
          //1,关联的源数据,商品的品牌
          val tmSql = "select id as tm_id,tm_name from gmall2020_base_trademark"
          val tmList: List[JSONObject] = PhoenixUtil.queryList(tmSql)
          //将JSON转换为Map,用ID作为key,方便后续的关联操作
          //TODO:优化
          val tmMap: Map[String, BaseTrademark] = tmList.map(jsonObj => {
            val o = jsonObj.toJavaObject(classOf[BaseTrademark])
            (o.tm_id,o )
          }).toMap

          //category3
          val category3Sql = "select id,name from gmall2020_base_category3"
          val category3List: List[JSONObject] = PhoenixUtil.queryList(category3Sql)
          //TODO:优化
          val category3Map: Map[String, BaseCategory3] = category3List.map(jsonObj => {
            val o = jsonObj.toJavaObject(classOf[BaseCategory3])
            (o.id, o)
          }).toMap

          //spu
          val spuSql = "select id,spu_name from gmall2020_spu_info"
          val spuList: List[JSONObject] = PhoenixUtil.queryList(spuSql)
          //TODO:优化
          val spuMap: Map[String, SpuInfo] = spuList.map(jsonObj => {
            val o = jsonObj.toJavaObject(classOf[SpuInfo])
            (o.id, o)
          }).toMap

          //汇总到一个List广播这个map
          val dimList: List[Map[String, Any]] = List[Map[String, Any]](category3Map, tmMap, spuMap)
          val dimBC: Broadcast[List[Map[String, Any]]] = ssc.sparkContext.broadcast(dimList)
          val skuInfoRDD: RDD[SkuInfo] = rdd.mapPartitions {
            skuInfoItr: Iterator[SkuInfo] => {
              val dimList: List[Map[String, Any]] = dimBC.value //接收bc
              val category3Map: Map[String, Any] = dimList(0)
              val tmMap: Map[String, Any] = dimList(1)
              val spuMap: Map[String, Any] = dimList(2)
              val skuInfoList: List[SkuInfo] = skuInfoItr.toList
              //用一个临时变量,保存迭代器的列表值
              for (skuInfo <- skuInfoList) {
                //category3,从map中寻值
                val category3JsonObj:BaseCategory3  = category3Map.getOrElse(skuInfo.category3_id, BaseCategory3.emptyObj).asInstanceOf[BaseCategory3]
                skuInfo.category3_name = category3JsonObj.name
                //tm,从map中寻值
                val tmJsonObj: BaseTrademark = tmMap.getOrElse(skuInfo.tm_id, BaseTrademark.emptyObj).asInstanceOf[BaseTrademark]
                skuInfo.tm_name = tmJsonObj.tm_name
                //spu,,从map中寻值
                val spuJsonObj: SpuInfo = spuMap.getOrElse(skuInfo.spu_id, SpuInfo.emptyObj).asInstanceOf[SpuInfo]
                skuInfo.spu_name = spuJsonObj.spu_name
              }
              skuInfoList.toIterator
            }
          }
          skuInfoRDD
          //        }
        }
      }
      //保存到hbase
      import org.apache.phoenix.spark._
      skuInfoDStream.foreachRDD {
        rdd => {
          //        rdd.cache()
          //        println("count=" + rdd.count())
          rdd.saveToPhoenix(
            "gmall2020_sku_info",
            Seq("ID", "SPU_ID", "PRICE", "SKU_NAME", "TM_ID", "CATEGORY3_ID", "CREATE_TIME", "CATEGORY3_NAME", "SPU_NAME", "TM_NAME"),
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
