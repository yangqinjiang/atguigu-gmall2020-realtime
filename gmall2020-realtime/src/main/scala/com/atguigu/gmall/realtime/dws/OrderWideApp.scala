package com.atguigu.gmall.realtime.dws

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.atguigu.gmall.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.atguigu.gmall.realtime.config.ApplicationConfig
import com.atguigu.gmall.realtime.utils.{MyKafkaSink, MyKafkaUtil, MyRedisUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.lang
import java.util.Properties
import scala.collection.mutable.ListBuffer

/**
 * 订单和订单明细双流合并
 */
object OrderWideApp {
  //TODO: 代码优化,重用
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]")
      .setAppName("OrderWideApp").set("spark.testing.memory", "2147480000")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    //两个流的kafka主题名称,消费组名
    //订单
    val orderInfoTopic = "dwd_order_info" //dwd_order_info
    val orderInfoGroupId = "dwd_order_info_group"
    //订单明细
    val orderDetailTopic = "dwd_order_detail" //dwd_order_detail
    val orderDetailGroupId = "dwd_order_detail_group"

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
    //    orderInfoDStream.print(1000)
    //    orderDetailDStream.print(1000)

    //以下的合流的方式是错误的
    //在流中 ,直接join是会出问题的,因为无法保证同一批次的订单和订单明细在同一个采集周期中

    /*
      //转换订单和订单明细结构为k-v类型,然后进行join
      val orderInfoWithKeyDStream: DStream[(Long, OrderInfo)] = orderInfoDStream.map(orderInfo => (orderInfo.id, orderInfo))
      val orderDetailWithKeyDStream: DStream[(Long, OrderDetail)] = orderDetailDStream.map(orderDetail => (orderDetail.order_id, orderDetail))
      val joinedDStream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyDStream.join(orderDetailWithKeyDStream, 4)
    */

    //=============开窗+去重=完成join==============
    //开窗指定窗口大小和滑动步长
    val orderInfoWindowDStream: DStream[OrderInfo] = orderInfoDStream.window(Seconds(50), Seconds(5))
    val orderDetailWindowDStream: DStream[OrderDetail] = orderDetailDStream.window(Seconds(50), Seconds(5))
    //join之前, 先转换结构,以key-value为结构, key是order_id
    val orderInfoWithKeyDStream: DStream[(Long, OrderInfo)] = orderInfoWindowDStream.map(orderInfo => (orderInfo.id, orderInfo))
    val orderDetailWithKeyDStream: DStream[(Long, OrderDetail)] = orderDetailWindowDStream.map(orderDetail => (orderDetail.order_id, orderDetail))
    val joinedDStream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyDStream.join(orderDetailWithKeyDStream, 4)
    //下面的算法是核心功能
    //去重 ,数据统一保存到 redis,数据类型为set, API是 sadd
    // order_join:[order_id] value=orderDetailId , 过期时间为600s
    // 其中,redis的sadd方法,返回是0,则说明待添加的元素已经存在set中,则过滤它,认为它是重复元素, 不再关联它
    val orderWideDStream: DStream[OrderWide] = joinedDStream.mapPartitions {
      tupleItr: Iterator[(Long, (OrderInfo, OrderDetail))] => {
        //使用redis的set实现去重
        val jedis: Jedis = MyRedisUtil.getJedisClient
        //临时的可变集合变量,保存关联后的数据
        val orderWideList: ListBuffer[OrderWide] = ListBuffer[OrderWide]()
        //临时变量,保存一份, 避免使用迭代器时,出现只访问一遍的问题
        val ool: List[(Long, (OrderInfo, OrderDetail))] = tupleItr.toList
        for ((orderId, (orderInfo, orderDetail)) <- ool) {
          val key = s"order_join:${orderId}"
          val ifNotExisted: lang.Long = jedis.sadd(key, orderDetail.id.toString)
          jedis.expire(key, 600)
          //如果不是重复关联,则合并宽表
          if (1L == ifNotExisted) {
            orderWideList.append(new OrderWide(orderInfo, orderDetail))
          }
        }
        //
        jedis.close()
        orderWideList.toIterator
      }
    }
    //    orderWideDStream.print(1000)

    // ------------实付分摊计算--------------

    val orderWideWithSplitDStream: DStream[OrderWide] = orderWideDStream.mapPartitions {
      orderWideItr => {
        //建立连接
        val jedis: Jedis = MyRedisUtil.getJedisClient
        //临时集合
        val orderWideList: List[OrderWide] = orderWideItr.toList
        //        println("分区orderIds:" + orderWideList.map(_.order_id).mkString(","))
        for (orderWide <- orderWideList) {

          //从Redis中获取原始金额累计
          //redis,String类型,Key=order_origin_sum:[order_id] ,value= Σ其他的明细(个数*单价)
          val originSumKey = "order_origin_sum:" + orderWide.order_id
          var orderOriginSum: Double = 0D
          //从redis拿到上次的累加值
          val orderOriginSumStr = jedis.get(originSumKey)
          //从 redis 中取出来的任何值都要进行判空
          if (null != orderOriginSumStr && orderOriginSumStr.nonEmpty) {
            orderOriginSum = orderOriginSumStr.toDouble
          }

          //从redis中获取分摊金额累计
          //redis,String类型,Key=order_split_sum:[order_id] ,value= Σ其他的明细的分摊金额
          val splitSumKey = "order_split_sum:" + orderWide.order_id
          var orderSplitSum: Double = 0D
          val orderSplitSumStr: String = jedis.get(splitSumKey)
          if (null != orderSplitSumStr && orderSplitSumStr.nonEmpty) {
            orderSplitSum = orderSplitSumStr.toDouble
          }
          //判断是否是最后一笔
          //如果当前 的一笔(个数*单价) == 原始总金额 - Σ其他的明细之和(个数*单价)
          // 个数 单价 原始金额 可以从 orderWide 取到，明细汇总值已从 Redis 取出

          //如果等式成立,说明 该笔明细是最后一笔,
          val detailAmount = orderWide.sku_price * orderWide.sku_num
          val isLastOrderDetail = detailAmount == (orderWide.original_total_amount - orderOriginSum)
          if (isLastOrderDetail) {
            // 分摊计算公式 :减法公式 分摊金额= 实际付款金额- Σ其他的明细的分摊金额 (减
            //法，适用最后一笔明细）
            // 实际付款金额 在 orderWide 中，要从 redis 中取得 Σ其他的明细的分摊金额
            orderWide.final_detail_amount =
              Math.round((orderWide.final_total_amount - orderSplitSum) * 100D) / 100D
            /*保持浮点数有两位*/
          } else {
            //如果不成立
            // 分摊计算公式： 乘除法公式： 分摊金额= 实际付款金额 *(个数*单价) / 原始总金额
            //（乘除法，适用非最后一笔明细)
            // 所有计算要素都在 orderWide 中，直接计算即可
            orderWide.final_detail_amount =
              Math.round((orderWide.final_total_amount * detailAmount / orderWide.original_total_amount) * 100D) / 100D /*保持浮点数有两位*/
          }
          // 分摊金额计算完成以后
          // 将本次计算的分摊金额 累计到 redis Σ其他的明细的分摊金额
          val newOrderSplitSum = (orderWide.final_detail_amount +
            orderSplitSum).toString
          jedis.setex(splitSumKey, 600, newOrderSplitSum)

          // 应付金额（单价*个数) 要累计到 Redis Σ其他的明细（个数*单价）
          val newOrderOriginSum = (detailAmount + orderOriginSum).toString
          jedis.setex(originSumKey, 600, newOrderOriginSum)
        }
        jedis.close()
        orderWideList.toIterator
      }
    }

//    orderWideWithSplitDStream.cache()
//    //测试输出到控制台
//    orderWideWithSplitDStream.map {
//      orderWide => JSON.toJSONString(orderWide, new SerializeConfig(true))
//    }.print(1000)
    //注意 cache流的数据
    //输出到clickHouse
    val sparkSession: SparkSession = SparkSession.builder().appName("order_detail_wide_spark_app").getOrCreate()
    import sparkSession.implicits._
    orderWideWithSplitDStream.foreachRDD {
      rdd => {
        rdd.cache() //注意,因为下面多次action,需要缓存此rdd的数据
        val df: DataFrame = rdd.toDF()
        df.write.mode(SaveMode.Append)
          .option("batchsize", "100")
          .option("isolationLevel", "NONE") //设置事务
          .option("numPartitions", "4") //设置并发
          .option("driver", ApplicationConfig.CLICKHOUSE_DRIVER)
          .jdbc(ApplicationConfig.CLICKHOUSE_URL, "t_order_wide_2020", new Properties())

        //将数据写回到 Kafka
        rdd.foreach{orderWide=>
          MyKafkaSink.send("dws_order_wide", JSON.toJSONString(orderWide,new SerializeConfig(true)))
        }
        //提交两个流的偏移量
        OffsetManagerUtil.saveOffset(orderInfoTopic, orderInfoGroupId, orderInfoOffsetRanges)
        OffsetManagerUtil.saveOffset(orderDetailTopic, orderDetailGroupId, orderDetailOffsetRanges)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
