package com.atguigu.gmall.realtime.common

import com.atguigu.gmall.realtime.utils.{MyKafkaUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Duration, StreamingContext}

trait RTApp extends Logging {

  //属性, 因为offsetRanges在DStream.transform周期性被修改,所以要提取到类属性中
  protected var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]

  def start(master: String = "local[*]"
            , topic: String = "RTTopic"
            , groupId: String = "RTGroupId",
            batchDuration: Duration)(offsetDStreamOp: (DStream[ConsumerRecord[String, String]], String, String) => Unit): Unit = {

    logWarning(this.getClass.getSimpleName + "开始运行了~~")
    val sparkConf: SparkConf = new SparkConf().setMaster(master)
      .setAppName(this.getClass.getSimpleName).set("spark.testing.memory", "2147480000")
    val ssc = new StreamingContext(sparkConf, batchDuration)
    //============消费kafka数据基本实现===================

    //从Redis中读取kafka偏移量
    val kafkaOffsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (kafkaOffsetMap != null && kafkaOffsetMap.nonEmpty) {
      //Redis中有偏移量,根据Redis中保存的偏移量读取
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, kafkaOffsetMap, groupId)
    } else {
      //Redis中没有保存偏移量,kafka默认从最新读取
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }
    //得到本批次中处理数据的分区对应的偏移量起始及结束位置
    // 注意：这里我们从 Kafka 中读取数据之后，直接就获取了偏移量的位置，因为 KafkaRDD 可以转换为
    //HasOffsetRanges，会自动记录位置
    //transform的使用方法?
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        //此处的代码在driver执行,所以能直接给offsetRanges赋值, 不用序列化操作
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    try {
      offsetDStreamOp(offsetDStream, topic, groupId)
    } catch {
      case ex: Throwable => println(ex.getMessage)
    }

    //TODO:配置优雅停机
    //启动
    ssc.start()
    ssc.awaitTermination()
  }
}
