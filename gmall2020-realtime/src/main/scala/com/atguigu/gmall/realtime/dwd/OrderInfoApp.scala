package com.atguigu.gmall.realtime.dwd

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.bean.{OrderInfo, ProvinceInfo, UserStatus}
import com.atguigu.gmall.realtime.common.{RTApp, StartConf}
import com.atguigu.gmall.realtime.utils._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.text.SimpleDateFormat
import java.util.Date

/**
 * 判断是否为首单用户实现
 */
object OrderInfoApp extends App with RTApp {
  val conf = StartConf("local[3]",
    "ods_order_info", "order_info_group", Seconds(5))
  //启动应用程序
  start(conf) {
    (offsetDStream: DStream[ConsumerRecord[String, String]],
     topic: String, groupId: String) => {
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
       * ➢ 漏洞
       * 如果一个用户是首次消费，在一个采集周期中，这个用户下单了 2 次，那么就会
       * 把这同一个用户都会统计为首单消费
       * ➢ 解决办法
       * 应该将同一采集周期的同一用户的最早的订单标记为首单，其它都改为非首单
       * ◼ 同一采集周期的同一用户-----按用户分组（ groupByKey）
       * ◼ 最早的订单-----排序，取最早（ sortwith）
       * ◼ 标记为首单-----具体业务代码
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

      //==============5.订单与 Hbase 中的维度表进行关联=============
      //5.1关联省份方案1, 以分区为单位进行关联
      // 适用场景:driver端内存比较小, 要关联的维度表数据量比较大
      val orderInfoWithProvinceDStream1: DStream[OrderInfo] = orderInfoRealWithFirstFlagDStream.mapPartitions {
        orderInfoItr: Iterator[OrderInfo] => {
          //迭代器 转换为 列表, 避免迭代器只能迭代一次的问题
          val orderInfoList: List[OrderInfo] = orderInfoItr.toList
          //获取本批次中所有订单省份的ID
          val provinceIdList: List[Long] = orderInfoList.map(_.province_id)
          //根据省份id到Hbase省份表中获取省份信息
          //注意id是string类型, 拼接字符串时, 要用单引号括起来
          var sql: String = s"select id,name,area_code,iso_code from gmall2020_province_info where id in ('${provinceIdList.mkString("','")}')"
          //返回的结果,如[{"id":"1","name":"zs","area_code":"1000","iso_code":"CN-JX"},....]
          val provinceJsonList: List[JSONObject] = PhoenixUtil.queryList(sql)
          //将provinceInfoList转换为Map集合
          //[id->{"id":"1","name":"zs","area_code":"1000","iso_code":"CN-JX"}]
          val provinceJsonMap: Map[Long, JSONObject] = provinceJsonList.map {
            proJsonObj => {
              //因为OrderInfo样例类的province_id是Long,所以id要转为Long
              //TODO:优化, 下面的proJsonObj,可以用JSON.parseObject(String,classOf[T]) 转为ProvinceInfo样例类
              (proJsonObj.getLongValue("ID"), proJsonObj)
            }
          }.toMap
          //这里开始, 将订单与省份数据关联起来
          for (orderInfo <- orderInfoList) {
            val provinceObj: JSONObject = provinceJsonMap.getOrElse(orderInfo.province_id, null)
            if (null != provinceObj) {
              orderInfo.province_iso_code = provinceObj.getString("ISO_CODE")
              orderInfo.province_name = provinceObj.getString("NAME")
              orderInfo.province_area_code = provinceObj.getString("AREA_CODE")
            }
          }
          orderInfoList.toIterator
        }
      }
      //    //有多个action算子时, 使用cache
      //    orderInfoWithProvinceDStream1.cache()
      //    orderInfoWithProvinceDStream1.print(1000)

      //5.1关联省份方案2,使用广播变量,在driver端进行一次查询,分区越多效果越明显,
      //前提是, 广播变量的数据量较小
      val orderInfoWithProvinceDStream2: DStream[OrderInfo] = orderInfoRealWithFirstFlagDStream.transform {
        rdd: RDD[OrderInfo] => {
          //每一个采集周期,都会在driver端执行从hbase中查询身份信息
          var sql: String = "select id,name,area_code,iso_code from gmall2020_province_info";
          val provinceInfoList: List[JSONObject] = PhoenixUtil.queryList(sql)
          //封装广播变量
          val provinceInfoMap: Map[String, ProvinceInfo] = provinceInfoList.map {
            jsonObj: JSONObject => {
              //TODO:优化, 下面的proJsonObj,可以用JSON.parseObject(String,classOf[T]) 转为ProvinceInfo样例类
              val provinceInfo = ProvinceInfo(
                jsonObj.getString("ID"),
                jsonObj.getString("NAME"),
                jsonObj.getString("AREA_CODE"),
                jsonObj.getString("ISO_CODE"),
              )
              (provinceInfo.id, provinceInfo)
            }
          }.toMap

          val provinceInfoBC: Broadcast[Map[String, ProvinceInfo]] = ssc.sparkContext.broadcast(provinceInfoMap)

          val orderInfoWithProvinceRDD: RDD[OrderInfo] = rdd.map {
            orderInfo: OrderInfo => {
              val provinceInfo: ProvinceInfo = provinceInfoBC.value.getOrElse(orderInfo.province_id.toString, null)
              if (null != provinceInfo) {
                orderInfo.province_name = provinceInfo.name
                orderInfo.province_area_code = provinceInfo.area_code
                orderInfo.province_iso_code = provinceInfo.iso_code
              }
              orderInfo
            }
          }
          orderInfoWithProvinceRDD //返回转换后的rdd
        }
      }
      //有多个action算子时, 使用cache
      //    orderInfoWithProvinceDStream2.cache()
      //    orderInfoWithProvinceDStream2.print(1000)

      //5.2关联用户
      val orderInfoWithUserDStream: DStream[OrderInfo] = orderInfoWithProvinceDStream2.mapPartitions {
        orderInfoItr: Iterator[OrderInfo] => {
          val orderInfoList: List[OrderInfo] = orderInfoItr.toList
          //获取用户id,生成列表,用于查询hbase
          val userIdList: List[Long] = orderInfoList.map(_.user_id)
          //根据用户id到phoenix中查询用户
          var sql: String = s"select id,user_level,birthday,gender,age_group,gender_name from gmall2020_user_info where id in ('${userIdList.mkString("','")}')"
          val userJsonList: List[JSONObject] = PhoenixUtil.queryList(sql)
          //getLongValue ,不会返回null值
          val userJsonMap: Map[Long, JSONObject] = userJsonList.map(userJsonObj => (userJsonObj.getLongValue("ID"), userJsonObj)).toMap
          for (orderInfo <- orderInfoList) {

            val userJsonObj: JSONObject = userJsonMap.getOrElse(orderInfo.user_id, null)
            if (null != userJsonObj) {
              orderInfo.user_gender = userJsonObj.getString("GENDER_NAME")
              orderInfo.user_age_group = userJsonObj.getString("AGE_GROUP")
            }
          }
          orderInfoList.toIterator
        }
      }
      //    orderInfoWithUserDStream.cache()
      //    orderInfoWithUserDStream.print(1000)
      //==============3保存用户状态============
      import org.apache.phoenix.spark._
      orderInfoWithUserDStream.foreachRDD {
        rdd: RDD[OrderInfo] => {
          //从所有订单中,将首单的订单过滤出来, 减少hbase操作的数据量
          val firstOrderRDD: RDD[OrderInfo] = rdd.filter(_.if_first_order == "1")
          //获取当前 订单并更新到Hbase,注意,saveToPhoenix在更新的时候, 要求rdd中的属性和插入hbase表中的列数必须保持一致,
          //所以转换一下
          val firstOrderUserRDD: RDD[UserStatus] = firstOrderRDD.map {
            orderInfo => UserStatus(orderInfo.user_id.toString, "1")
          }
          firstOrderUserRDD.saveToPhoenix(
            "USER_STATUS2020",
            Seq("USER_ID", "IF_CONSUMED"), //注意,字段要大写
            new org.apache.hadoop.conf.Configuration,
            Some("hadoop102,hadoop103,hadoop104:2181")
          )

          //--------------3.2 将订单信息写入到 ES 中-----------------
          rdd.foreachPartition {
            orderInfoItr => {
              //======下面将订单信息写入到ES, kafka的dwd层==========
              val orderInfoList: List[(String, OrderInfo)] =
                orderInfoItr.toList.map(orderInfo => (orderInfo.id.toString, orderInfo))
              val dateStr: String = new SimpleDateFormat("yyyyMMdd").format(new Date())
              MyESUtil.bulkInsert(orderInfoList, "gmall2020_order_info_" + dateStr)

              //3.2 将订单信息推回 kafka 进入下一层处理 主题： dwd_order_info
              for ((id, orderInfo) <- orderInfoList) {
                //fastjson 要把 scala 对象包括 caseclass 转 json 字符串 需要加入,new SerializeConfig(true)
                MyKafkaSink.send("dwd_order_info",
                  JSON.toJSONString(orderInfo, new SerializeConfig(true)))
              }
            }
          }

          //保存hbase数据后,再保存偏移量
          OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
        }
      }

    }
  }
}
