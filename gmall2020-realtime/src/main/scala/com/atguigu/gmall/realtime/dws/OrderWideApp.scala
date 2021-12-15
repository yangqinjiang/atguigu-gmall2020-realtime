package com.atguigu.gmall.realtime.dws

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.realtime.bean.{OrderDetail, OrderInfo}
import com.atguigu.gmall.realtime.utils.{MyKafkaUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 订单和订单明细双流合并
 */
object OrderWideApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]")
      .setAppName("OrderWideApp").set("spark.testing.memory", "2147480000")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    //两个流的kafka主题名称,消费组名
    //订单
    val orderInfoTopic = "dws_order_info"
    val orderInfoGroupId = "dws_order_info_group"
    //订单明细
    val orderDetailTopic = "dws_order_detail"
    val orderDetailGroupId = "dws_order_detail_group"

    //从redis中读取 偏移量(这是启动时执行一次)
    val orderInfoOffsetMapForKafka: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(orderInfoTopic, orderInfoGroupId)
    val orderDetailOffsetMapForKafka: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(orderDetailTopic, orderDetailGroupId)

    //根据订单偏移量, 从kafka中获取订单数据
    var orderInfoRecordInputDStream: InputDStream[ConsumerRecord[String, String]] = null
    //根据是否能取到偏移量来决定如何加载kafka流
    if (orderInfoOffsetMapForKafka != null && orderInfoOffsetMapForKafka.nonEmpty) {
      orderInfoRecordInputDStream = MyKafkaUtil.getKafkaStream(orderInfoTopic, ssc, orderInfoOffsetMapForKafka, orderInfoGroupId)
    } else {
      orderInfoRecordInputDStream = MyKafkaUtil.getKafkaStream(orderInfoTopic, ssc, orderInfoGroupId)
    }

    //根据订单明细偏移量,从kafka中获取订单明细数据
    var orderDetailRecordInputDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderDetailOffsetMapForKafka != null && orderDetailOffsetMapForKafka.nonEmpty) {
      orderDetailRecordInputDStream = MyKafkaUtil.getKafkaStream(orderDetailTopic, ssc, orderDetailOffsetMapForKafka, orderDetailGroupId)
    } else {
      orderDetailRecordInputDStream = MyKafkaUtil.getKafkaStream(orderDetailTopic, ssc, orderDetailGroupId)
    }

    //订单:从流中获得本批次的订单偏移量结束点(每批次执行一次)
    //周期性储存了当前批次偏移量的变化状态, 重要的是偏移量结束点
    var orderInfoOffsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val orderInfoInputGetOffsetDStream: DStream[ConsumerRecord[String, String]] = orderInfoRecordInputDStream.transform {
      rdd => //周期性在driver中执行
        orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
    }

    //订单明细:从流中获得本批次的订单偏移量结束点(每批次执行一次)
    //周期性储存了当前批次偏移量的变化状态, 重要的是偏移量结束点
    var orderDetailOffsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val orderDetailInputGetOffsetDStream: DStream[ConsumerRecord[String, String]] = orderDetailRecordInputDStream.transform {
      rdd => //周期性在driver中执行
        orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
    }

    //提取订单数据
    val orderInfoDStream: DStream[OrderInfo] = orderInfoInputGetOffsetDStream.map {
      record => {
        val jsonString: String = record.value()
        val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
        orderInfo
      }
    }

    //提取订单明细数据
    val orderDetailDStream: DStream[OrderDetail] = orderDetailInputGetOffsetDStream.map {
      record => {
        val jsonString: String = record.value()
        val orderDetail: OrderDetail = JSON.parseObject(jsonString, classOf[OrderDetail])
        orderDetail
      }
    }
    orderInfoDStream.print(1000)
    orderDetailDStream.print(1000)

    //以下的合流的方式是错误的
    //在流中 ,直接join是会出问题的,因为无法保证同一批次的订单和订单明细在同一个采集周期中

    /*
      //转换订单和订单明细结构为k-v类型,然后进行join
      val orderInfoWithKeyDStream: DStream[(Long, OrderInfo)] = orderInfoDStream.map(orderInfo => (orderInfo.id, orderInfo))
      val orderDetailWithKeyDStream: DStream[(Long, OrderDetail)] = orderDetailDStream.map(orderDetail => (orderDetail.order_id, orderDetail))
      val joinedDStream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyDStream.join(orderDetailWithKeyDStream, 4)
    */

    ssc.start()
    ssc.awaitTermination()
  }
}
