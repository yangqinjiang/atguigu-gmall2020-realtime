package com.atguigu.gmall.realtime.dwd

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.bean.OrderDetail
import com.atguigu.gmall.realtime.common.{RTApp, StartConf}
import com.atguigu.gmall.realtime.utils.{MyKafkaSink, OffsetManagerUtil, PhoenixUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

object OrderDetailApp extends App with RTApp {
  val conf = StartConf("local[3]",
    "ods_order_detail", "order_detail_group", Seconds(5))
  //启动应用程序
  start(conf) {
    (offsetDStream: DStream[ConsumerRecord[String, String]],
     topic: String, groupId: String) => {
      //提取数据
      val orderDetailDStream: DStream[OrderDetail] = offsetDStream.map {
        record => {
          val jsonString: String = record.value()
          //订单处理,转换成更方便操作的专用样例类
          val orderDetail: OrderDetail = JSON.parseObject(jsonString, classOf[OrderDetail])
          orderDetail
        }
      }

      //    orderDetailDStream.print(1000)

      //订单明细事实表与商品维表数据关联,品牌,分类spu
      val orderDetailWithSkuDstream: DStream[OrderDetail] = orderDetailDStream.mapPartitions {
        orderDetailItr: Iterator[OrderDetail] => {
          //临时变量
          val orderDetailList: List[OrderDetail] = orderDetailItr.toList
          if (orderDetailList.nonEmpty) {
            val skuIdList: List[Long] = orderDetailList.map(_.sku_id)
            //从hbase查询指定的skuId
            val sql = s"select id,tm_id,spu_id,category3_id,tm_name,spu_name,category3_name from gmall2020_sku_info where id in ('${skuIdList.mkString("','")}')"
            val skuJsonObjList: List[JSONObject] = PhoenixUtil.queryList(sql)
            val skuJsonObjMap: Map[Long, JSONObject] = skuJsonObjList.map(skuJsonObj => (skuJsonObj.getLongValue("ID"), skuJsonObj)).toMap
            for (orderDetail <- orderDetailList) {
              val skuJsonObj: JSONObject = skuJsonObjMap.getOrElse(orderDetail.sku_id, null)
              //关联orderDetail与sku的数据
              orderDetail.spu_id = skuJsonObj.getLong("SPU_ID")
              orderDetail.spu_name = skuJsonObj.getString("SPU_NAME")
              orderDetail.tm_id = skuJsonObj.getLong("TM_ID")
              orderDetail.tm_name = skuJsonObj.getString("TM_NAME")
              orderDetail.category3_id = skuJsonObj.getLong("CATEGORY3_ID")
              orderDetail.category3_name = skuJsonObj.getString("CATEGORY3_NAME")
            }
          }
          orderDetailList.toIterator
        }
      }
      //    orderDetailWithSkuDstream.print(1000)
      //    if(offsetRanges != null)
      //      OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      //将关联后的订单明细宽表写入到kafka的dwd层
      orderDetailWithSkuDstream.foreachRDD {
        rdd: RDD[OrderDetail] => {
          rdd.foreach {
            orderDetail: OrderDetail => {
              MyKafkaSink.send("dwd_order_detail", JSON.toJSONString(orderDetail, new SerializeConfig(true)))
            }
          }
          OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
        }
      }
    }
  }

}
