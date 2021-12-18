package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.bean.DauInfo
import com.atguigu.gmall.realtime.common.RTApp
import com.atguigu.gmall.realtime.utils.{MyESUtil, MyRedisUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

import java.lang
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

/**
 * DailyActiveUser 日活用户统计业务类
 */
object DauApp extends App with RTApp {
  //启动应用程序
  start("local[3]",
    "gmall_start_0523", "gmall_dau_group", Seconds(5)) {
    (offsetDStream: DStream[ConsumerRecord[String, String]],
     topic: String, groupId: String) => {

      //测试输出1
      //    recordDstream.map(_.value()).print(100)

      val jsonObjDStream: DStream[JSONObject] = offsetDStream.map {
        record =>
          //获取启动日志
          val jsonStr: String = record.value()
          //将启动日志转换为json对象
          val jsonObj: JSONObject = JSON.parseObject(jsonStr)
          //获取时间戳, 毫秒数
          val ts: lang.Long = jsonObj.getLong("ts")
          //获取字符串  日期 小时
          val dateHourString: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
          //对字符串日期和小时进行分割, 分割后放到json对象中,方便后续处理
          val dateHour: Array[String] = dateHourString.split(" ")
          jsonObj.put("dt", dateHour(0))
          jsonObj.put("hr", dateHour(1))
          jsonObj
      }
      //测试输出 2
      //    jsonObjDStream.print(100)

      //===================使用Redis进行去重=================
      //方案1,缺点,虽然我们从池中获取Redis,但是每次从流中取数据都进行过滤
      //连接还是过于频繁
      //    val filteredDStream: DStream[JSONObject] = jsonObjDStream.filter {
      //      jsonObj => {
      //        //获取当前日期
      //        val dt: String = jsonObj.getString("dt")
      //        //获取设备mid
      //        val mid: String = jsonObj.getJSONObject("common").getString("mid")
      //        //获取Redis客户端
      //        val jedisClient: Jedis = MyRedisUtil.getJedisClient
      //        //拼接向Redis放的数据的key
      //        val dauKey: String = "dau:" + dt
      //        //判断Redis中是否存在该数据
      //        val isNew: lang.Long = jedisClient.sadd(dauKey, mid)
      //        //设置当天的key数据失效时间为24小时
      //        //避免多次重新赋值
      //        if (jedisClient.ttl(dauKey) < 0) {
      //          //设置当天的key数据失效时间为24小时
      //          jedisClient.expire(dauKey, 3600 * 24)
      //        }
      //        jedisClient.close()
      //        if (isNew == 1L) {
      //          //Redis不存在, 我们需要从DS流中将数据过滤出来,同时数据会保存到Redis中
      //          true
      //        } else {
      //          //Redis中已经存在该数据, 我们需要把数据从DS流中过滤掉
      //          false
      //        }
      //      }
      //    }
      //输出测试, 数量会越来越少, 最后变为0,因为我们mid只是模拟了50个
      //    filteredDStream.count().print()
      //方案2 以分区为单位进行过滤,可以减少和连接池交互的次数
      val filteredDStream2 = jsonObjDStream.mapPartitions {
        jsonObjItr => {
          //获取Redis客户端
          val jedisClient = MyRedisUtil.getJedisClient
          //定义当前分区过滤后的数据
          val filteredList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]
          for (jsonObj <- jsonObjItr) {
            //获取当前日期
            //获取当前日期
            val dt: String = jsonObj.getString("dt")
            //获取设备mid
            val mid: String = jsonObj.getJSONObject("common").getString("mid")
            //获取Redis客户端
            val jedisClient: Jedis = MyRedisUtil.getJedisClient
            //拼接向Redis放的数据的key
            val dauKey: String = "dau:" + dt

            //判断Redis中是否存在该数据
            val isNew: lang.Long = jedisClient.sadd(dauKey, mid)
            //避免多次重新赋值
            if (jedisClient.ttl(dauKey) < 0) {
              //设置当天的key数据失效时间为24小时
              jedisClient.expire(dauKey, 3600 * 24)
            }

            if (isNew == 1L) {
              //如果redis中不存在, 那么将数据添加到新建的ListBuffer集合中,实现过滤的效果
              filteredList.append(jsonObj)
            }
          }
          jedisClient.close()
          filteredList.toIterator
        }
      }
      //输出测试, 数量会越来越少, 最后变为0,
      //因为我们 mid 只是模拟了 50 个
      //    filteredDStream2.cache()
      //    filteredDStream2.count().print()

      //==============向ES中保存数据==========
      filteredDStream2.foreachRDD {
        rdd: RDD[JSONObject] => {
          rdd.foreachPartition { //以分区为单位对 RDD 中的数据进行处理， 方便批量插入
            jsonItr => {
              var dt: String = ""
              val dauList: List[(String, DauInfo)] = jsonItr.map {
                jsonObj => {
                  //每次处理的是一个 json 对象 将 json 对象封装为样例类
                  val commonJsonObj: JSONObject = jsonObj.getJSONObject("common")
                  dt = jsonObj.getString("dt") // 从源数据拿到日期
                  val info = DauInfo(
                    commonJsonObj.getString("mid"),
                    commonJsonObj.getString("uid"),
                    commonJsonObj.getString("ar"),
                    commonJsonObj.getString("ch"),
                    commonJsonObj.getString("vc"),
                    dt,
                    jsonObj.getString("hr"),
                    "00", //分钟我们前面没有转换，默认 00
                    jsonObj.getLong("ts")
                  )
                  (info.mid, info)
                }
              }.toList

              //对分区的数据进行批量处理
              //获取当前日志字符串
              //这里不应该拿到运行日期
              //  val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
              MyESUtil.bulkInsert(dauList, "gmall2020_dau_info_" + dt)
            }
          }
          //在保存最后提交偏移量
          OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
        }
      }

    }
  }
}
