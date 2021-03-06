package com.atguigu.gmall.realtime.common

import com.atguigu.gmall.realtime.config.ApplicationConfig
import com.atguigu.gmall.realtime.utils.{MyKafkaUtil, OffsetManagerUtil, StreamingUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.language.implicitConversions //引入反射

trait RTApp extends Logging {

  protected var ssc: StreamingContext = _
  //属性, 因为offsetRanges在DStream.transform周期性被修改,所以要提取到类属性中
  protected var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]

  /**
   * 获取kafka的消费偏移量
   *
   * @param topicName 主题
   * @param groupId   消费者组
   * @return
   */
  def getKafkaOffset(topicName: String, groupId: String): Map[TopicPartition, Long] = {
    OffsetManagerUtil.getOffset(topicName, groupId)
  }

  def start(conf: StartConf)(offsetDStreamOp: (DStream[ConsumerRecord[String, String]], String, String) => Unit): Unit = {

    val appName = this.getClass.getSimpleName.stripSuffix("$")
    logInfo(appName + "开始运行了~~")
    val sparkConf: SparkConf = new SparkConf()
      .setAppName(appName)
      .set("spark.streaming.stopGracefullyOnShutdown", "true") // 优雅停止spark服务
    if (ApplicationConfig.APP_LOCAL_MODE) {
      logWarning("运行模式为 本地")
      sparkConf.setMaster(ApplicationConfig.APP_SPARK_MASTER)
        //设置每批次消费数据最大数据量, 生成环境使用命令行设置
        .set("spark.streaming.kafka.maxRatePerPartition", "10000")
        .set("spark.testing.memory", "2147480000")
    }
    ssc = new StreamingContext(sparkConf, Seconds(conf.seconds))
    //============消费kafka数据基本实现===================

    //从Redis中读取kafka偏移量
    val kafkaOffsetMap: Map[TopicPartition, Long] = getKafkaOffset(conf.topic, conf.groupId)
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (kafkaOffsetMap != null && kafkaOffsetMap.nonEmpty) {
      //Redis中有偏移量,根据Redis中保存的偏移量读取
      recordDStream = MyKafkaUtil.getKafkaStream(conf.topic, ssc, kafkaOffsetMap, conf.groupId)
    } else {
      //Redis中没有保存偏移量,kafka默认从最新读取
      recordDStream = MyKafkaUtil.getKafkaStream(conf.topic, ssc, conf.groupId)
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
      offsetDStreamOp(offsetDStream, conf.topic, conf.groupId)
    } catch {
      case ex: Throwable => println(ex.getMessage)
    }

    //配置优雅停机
    //启动
    ssc.start()
    //通过扫描监控文件，优雅的关闭停止StreamingContext流式应用
    // 设置参数spark.streaming.stopGracefullyOnShutdown为true，优雅的关闭
    // 自动适应本地文件系统或者Hadoop的DFS系统,
    StreamingUtils.stopStreaming(ssc, "gmall2020-realtime/datas/stop/" + appName)
  }
}
