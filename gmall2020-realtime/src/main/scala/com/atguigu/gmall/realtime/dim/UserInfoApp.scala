package com.atguigu.gmall.realtime.dim

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.realtime.bean.UserInfo
import com.atguigu.gmall.realtime.utils.{MyKafkaUtil, OffsetManagerUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.text.SimpleDateFormat
import java.util.Date

/**
 * 从kafka中读取用户维度数据,写入到Hbase中
 */
object UserInfoApp {

  def main(args: Array[String]): Unit = {

    //1,从kafka中查询省份维度信息
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("UserInfoApp").set("spark.testing.memory", "2147480000")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ods_user_info"
    val groupId = "gmall_user_info_group"

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
    userInfoDStream.foreachRDD{
      userInfoRDD: RDD[UserInfo] =>{

        //保存到hbase
        import org.apache.phoenix.spark._
        userInfoRDD.saveToPhoenix(
          "gmall2020_user_info",
          Seq("ID", "USER_LEVEL", "BIRTHDAY", "GENDER","AGE_GROUP","GENDER_NAME"),
          new Configuration,
          Some("hadoop102,hadoop103,hadoop104:2181")
        )
        //处理完数据, 再保存偏移量
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }


    ssc.start()
    ssc.awaitTermination()
  }
}
