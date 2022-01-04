package com.atguigu.gmall.realtime.dim

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.realtime.bean.UserInfo
import com.atguigu.gmall.realtime.common.{RTApp, StartConf}
import com.atguigu.gmall.realtime.config.ApplicationConfig
import com.atguigu.gmall.realtime.utils.OffsetManagerUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import java.text.SimpleDateFormat
import java.util.Date

/**
 * 从kafka中读取用户维度数据,写入到Hbase中
 */
object UserInfoApp extends App with RTApp {
  val conf = StartConf("ods_user_info", "gmall_user_info_group")

  //启动应用程序
  start(conf) {
    (offsetDStream: DStream[ConsumerRecord[String, String]],
     topic: String, groupId: String) => {
      val userInfoDStream: DStream[UserInfo] = offsetDStream.map {
        record: ConsumerRecord[String, String] => { //record是从kafka读取出来 的
          val userInfoJsonStr: String = record.value()
          val userInfo: UserInfo = JSON.parseObject(userInfoJsonStr, classOf[UserInfo])
          //把生日转成年龄
          val formattor = new SimpleDateFormat("yyyy-MM-dd")
          val date: Date = formattor.parse(userInfo.birthday)
          val curTs: Long = System.currentTimeMillis()
          val betweenMs: Long = curTs - date.getTime
          val age: Long = betweenMs / 1000L / 60L / 60L / 24L / 365L // 毫秒  转为  年
          if (age < 20) {
            userInfo.age_group = "20岁及以下"
          } else if (age > 30) {
            userInfo.age_group = "30岁以上"
          } else {
            userInfo.age_group = "20岁到30岁"
          }
          if (userInfo.gender == "M") {
            userInfo.gender_name = "男"
          } else {
            userInfo.gender_name = "女"
          }
          userInfo
        }
      }
      //写入到hbase中
      userInfoDStream.foreachRDD {
        userInfoRDD: RDD[UserInfo] => {

          //保存到hbase
          import org.apache.phoenix.spark._
          userInfoRDD.saveToPhoenix(
            "gmall2020_user_info",
            Seq("ID", "USER_LEVEL", "BIRTHDAY", "GENDER", "AGE_GROUP", "GENDER_NAME"),
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
