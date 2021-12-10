package com.atguigu.gmall.realtime.dim

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.realtime.bean.ProvinceInfo
import com.atguigu.gmall.realtime.utils.{MyKafkaUtil, OffsetManagerUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 从kafka中读取省份维度数据,写入到Hbase中
 */
object ProvinceInfoApp {

  def main(args: Array[String]): Unit = {

    //1,从kafka中查询省份维度信息
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("ProvinceInfoApp").set("spark.testing.memory", "2147480000")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ods_base_province"
    val groupId = "gmall_province_info_group"

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
    //写入到hbase中
    offsetDStream.foreachRDD{
      rdd: RDD[ConsumerRecord[String, String]] =>{
        val provinceInfoRDD: RDD[ProvinceInfo] = rdd.map {
          record: ConsumerRecord[String, String] => {
            //得到从kafka中读取的jsonString
            val jsonString: String = record.value()
            //转换为ProvinceInfo样例类
            val provinceInfo: ProvinceInfo = JSON.parseObject(jsonString, classOf[ProvinceInfo])
            provinceInfo
          }
        }

        println("saveToPhoenix....")
        //保存到hbase
        import org.apache.phoenix.spark._
        provinceInfoRDD.saveToPhoenix(
          "gmall2020_province_info",
          Seq("ID","NAME","AREA_CODE","ISO_CODE"),
          new Configuration,
          Some("hadoop102,hadoop103,hadoop104:2181")
        )
        println("saveToPhoenix 2....")
        //处理完数据, 再保存偏移量
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }


    ssc.start()
    ssc.awaitTermination()
  }
}
