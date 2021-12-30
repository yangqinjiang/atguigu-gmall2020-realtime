package com.atguigu.gmall.realtime.utils

import com.atguigu.gmall.realtime.config.ApplicationConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable

/**
 * SparkStreaming 读取 kafka 数据工具类
 */
object MyKafkaUtil {

  //kafka消费者配置,
  // 注意: 因为ConsumerStrategies.Subscribe的参数类型要求,
  //        Map中的类型是String=>Object, 不能是Any或者其它类型.
  //      否则出现这样的错误: overloaded method value Subscribe with alternatives
  var kafkaParam: mutable.Map[String, Object] = collection.mutable.Map(
    "bootstrap.servers" -> ApplicationConfig.KAFKA_BOOTSTRAP_SERVERS, //用于初始化链接到集群的地址
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    //用于标识这个消费者属于哪个消费团体
    "group.id" -> "gmall2020_group",
    //latest自动重置偏移量为最新的偏移量
    "auto.offset.reset" -> ApplicationConfig.KAFKA_AUTO_OFFSET_RESET,
    //如果为true,则这个消费者的偏移量会在后台自动提交,但是kafka宕机容易丢失数据
    //如果是false,会需要手动维护kafka偏移量
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  //创建DStream,返回接收到的输入数据
  def getKafkaStream(topic: String, ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {
    val dStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam)
    )
    dStream
  }

  //创建DStream,返回接收到的输入数据,
  //指定groupId
  def getKafkaStream(topic: String, ssc: StreamingContext, groupId: String): InputDStream[ConsumerRecord[String, String]] = {
    kafkaParam("group.id") = groupId
    val dStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam)
    )
    dStream
  }

  //创建DStream,返回接收到的输入数据,
  //指定groupId,offsets偏移量
  def getKafkaStream(topic: String, ssc: StreamingContext, offsets: Map[TopicPartition, Long], groupId: String): InputDStream[ConsumerRecord[String, String]] = {
    kafkaParam("group.id") = groupId
    val dStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam, offsets)
    )
    dStream
  }
}
