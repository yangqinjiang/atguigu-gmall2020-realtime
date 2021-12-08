package com.atguigu.gmall.realtime.dwd

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.bean.{OrderInfo, UserStatus}
import com.atguigu.gmall.realtime.utils.{MyKafkaUtil, OffsetManagerUtil, PhoenixUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 判断是否为首单用户实现
 */
object OrderInfoApp {

  def main(args: Array[String]): Unit = {
    //1,从kafka中查询订单信息
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("OrderInfoApp").set("spark.testing.memory", "2147480000")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ods_order_info"
    val groupId = "order_info_group"

    //2从Redis中读取Kafka偏移量
    val kafkaOffsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(kafkaOffsetMap!=null && kafkaOffsetMap.nonEmpty){
      //Redis中有偏移量,根据Redis中保存的偏移量,读取
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, kafkaOffsetMap, groupId)
    }else{
      //Redis中没有保存偏移量,kafka默认从最新读取
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    //3 得到本批次中处理数据的分区对应 的偏移量起始及结束位置
    //注意: 这里我们从kafka中读取数据之后,直接就获取了偏移量的位置,因为kafkaRdd可以转换为HasOffsetRanges,会自动记录位置
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }
    //对从kafka中读取到的数据进行结构转换,由kafka的consumerRecord[String,String]
    // 转换为一个orderInfo对象
    val orderInfoDStream: DStream[OrderInfo] = offsetDStream.map {
      record => {
        val jsonString: String = record.value()
        val orderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
        //通过对创建时间2020-07-13 01:38:16进行拆分, 赋值给日期和小时属性
        //方便后续处理
        val createTimeArr: Array[String] = orderInfo.create_time.split(" ")
        //获取日期赋给日期属性
        orderInfo.create_date = createTimeArr(0)
        //获取小时赋值小时属性
        orderInfo.create_hour = createTimeArr(1).split(":")(0)
        orderInfo
      }
    }

    //----------------判断下单的用户是否为首单--------------
    //方案1,对DStream中的数据进行处理,判断下单的用户是否为首单
    //缺点, 每条订单数据都要执行一次SQL,SQL执行过于频繁
    val orderInfoWithFirstFlagDStream: DStream[OrderInfo] = orderInfoDStream.map {
      orderInfo => {
        //通过phoenix工具到hbase中查询用户状态
        val sql: String = s"select user_id,if_consumed from user_status2020 where user_id='${orderInfo.user_id}'"
        val userStatusList: List[JSONObject] = PhoenixUtil.queryList(sql)
        if (userStatusList != null && userStatusList.nonEmpty) {
          //如果数据表已有对应 的数据,则赋值为0
          orderInfo.if_first_order = "0" //此类型是字符串
        } else {
          //没有对应的数据,则赋值为1
          orderInfo.if_first_order = "1"
        }
        orderInfo
      }
    }
//    orderInfoWithFirstFlagDStream.print(1000)

    //方案2, 分区处理 mapPartitions
    //对DStream中数据进行处理, 判断下单的用户是否为首单
    //优化,以分区为单位, 将一个分区的查询操作改动为一条sql
    val orderInfoWithFirstFlagDStream2: DStream[OrderInfo] = orderInfoDStream.mapPartitions {
      //此参数为迭代器
      orderInfoItr: Iterator[OrderInfo] => {
        //因为迭代器迭代之后就获取不到数据了，所以将迭代器转换为集合进行操作
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList
        //获取当前分区内的用户 ids
        val userIdList: List[Long] = orderInfoList.map(_.user_id)
        //从hbase中查询整个分区的用户是否消费过,获取消费过的用户ids
        //坑1, 注意拼接sql时,分隔符
        val sql: String = s"select user_id,if_consumed from user_status2020 where user_id in ('${userIdList.mkString("','")}')"
        val userStatusList: List[JSONObject] = PhoenixUtil.queryList(sql)
        //得到已经消费过的用户的id集合
        //坑2,phoenix查询返回的字段名称全是大写
        val cosumedUserIdList: List[String] = userStatusList.map(_.getString("USER_ID"))
        //对分区数据进行遍历
        for (orderInfo <- orderInfoList) {
          //坑3: 注意： orderInfo 中 user_id 是 Long 类型，一定别忘了进行转换
          if (cosumedUserIdList.contains(orderInfo.user_id.toString)) {
            orderInfo.if_first_order = "0"
          } else {
            orderInfo.if_first_order = "1"
          }
        }
        orderInfoList.toIterator // 返回修改数据后的迭代器
      }
    }
//    orderInfoWithFirstFlagDStream2.print(1000)


    /**
     * TODO: 一个采集周期状态修正
      ➢ 漏洞
      如果一个用户是首次消费，在一个采集周期中，这个用户下单了 2 次，那么就会
      把这同一个用户都会统计为首单消费
      ➢ 解决办法
      应该将同一采集周期的同一用户的最早的订单标记为首单，其它都改为非首单
      ◼ 同一采集周期的同一用户-----按用户分组（ groupByKey）
      ◼ 最早的订单-----排序，取最早（ sortwith）
      ◼ 标记为首单-----具体业务代码
     */

    //===============4.同批次状态修正=================
    //因为要使用 groupByKey 对用户进行分组，所以先对 DStream 中的数据结构进行转换
    val orderInfoWithKeyDStream: DStream[(Long, OrderInfo)] = orderInfoWithFirstFlagDStream2.map {
      orderInfo => {
        (orderInfo.user_id, orderInfo)
      }
    }
    //按照用户id对当前采集周期数据进行分组
    val groupByKeyDStream: DStream[(Long, Iterable[OrderInfo])] = orderInfoWithKeyDStream.groupByKey()
    //对分组后的用户订单进行判断
    val orderInfoRealWithFirstFlagDStream: DStream[OrderInfo] = groupByKeyDStream.flatMap {
      case (userId, orderInfoItr) => {
        //如果同一批次, 某用户的订单数量大于1
        //说明前面的DStream会存在同一个用户, 有两个以上的订单被标记为 首单
        //所以,要处理下单时间最早的订单, 为首单, 剩下的为非首单
        if (orderInfoItr.size > 1) {
          //对用户订单按照时间进行排序,升降
          val sortedList: List[OrderInfo] = orderInfoItr.toList.sortWith(
            (orderInfo1, orderInfo2) => {
              orderInfo1.create_time < orderInfo2.create_time
            }
          )

          //获取排序后集合的第一个元素, 此时肯定存在一个元素,
          val orderInfoFirst: OrderInfo = sortedList(0)
          //判断是否为首单
          if (orderInfoFirst.if_first_order == "1") {
            //将除了首单的其它订单设置为非首单
            for (i <- 1 until sortedList.size) {
              val orderInfoNotFirst: OrderInfo = sortedList(i)
              orderInfoNotFirst.if_first_order = "0"
            }
          }
          sortedList
        } else {
          //当前用户在当前批次只有一个订单, 不会出现[多个订单全是首单]的情况
          orderInfoItr
        }
      }
    }

    //==============3保存用户状态============
    import org.apache.phoenix.spark._
    orderInfoRealWithFirstFlagDStream.foreachRDD{
      rdd => {
        //从所有订单中,将首单的订单过滤出来, 减少hbase操作的数据量
        val firstOrderRDD: RDD[OrderInfo] = rdd.filter(_.if_first_order == "1")
        //获取当前 订单并更新到Hbase,注意,saveToPhoenix在更新的时候, 要求rdd中的属性和插入hbase表中的列数必须保持一致,
        //所以转换一下
        val firstOrderUserRDD: RDD[UserStatus] = firstOrderRDD.map {
          orderInfo => UserStatus(orderInfo.user_id.toString, "1")
        }
        firstOrderUserRDD.saveToPhoenix(
          "USER_STATUS2020",
          Seq("USER_ID","IF_CONSUMED"),//注意,字段要大写
          new org.apache.hadoop.conf.Configuration,
          Some("hadoop102,hadoop103,hadoop104:2181")
        )
        //保存hbase数据后,再保存偏移量
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
