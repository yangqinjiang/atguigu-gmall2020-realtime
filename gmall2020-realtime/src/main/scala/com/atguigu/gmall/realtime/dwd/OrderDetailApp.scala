package com.atguigu.gmall.realtime.dwd

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.bean.OrderDetail
import com.atguigu.gmall.realtime.utils.{MyKafkaSink, MyKafkaUtil, OffsetManagerUtil, PhoenixUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderDetailApp {

  def main(args: Array[String]): Unit = {
    //加载流,手动保存偏移量
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("OrderDetailApp")
      .set("spark.testing.memory", "2147480000")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ods_order_detail"
    val groupId = "order_detail_group"


    var recordInputDStream: InputDStream[ConsumerRecord[String, String]] = null
    //从redis中读取偏移量
    val offsetMapForKafka: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    //通过偏移量到kafka中获取数据
    if (offsetMapForKafka != null && offsetMapForKafka.size > 0) {
      recordInputDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMapForKafka, groupId)
    } else {
      recordInputDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }
    //从流中获得本批次的偏移量结束点(每批次执行一次)
    var offsetRanges: Array[OffsetRange] = null //周期性储存了当前批次偏移量的变化状态,重要的是偏移量结束点
    val inputGetOffsetDStream: DStream[ConsumerRecord[String, String]] = recordInputDStream.transform {
      rdd: RDD[ConsumerRecord[String, String]] => {
        //周期性在driver中执行
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }
    //提取数据
    val orderDetailDStream: DStream[OrderDetail] = inputGetOffsetDStream.map {
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
    ssc.start()
    ssc.awaitTermination()
  }
}
