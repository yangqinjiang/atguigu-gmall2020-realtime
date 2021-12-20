package com.atguigu.gmall.realtime.utils

import com.atguigu.gmall.realtime.config.ApplicationConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties

/**
 * 向kafka中写入数据工具类
 */
object MyKafkaSink {

  //kafka消息生产者
  var kafkaProducer: KafkaProducer[String, String] = null

  //创建kafka消息生产者
  def createKafkaProducer: KafkaProducer[String, String] = {
    //配置
    val properties = new Properties()
    properties.put("bootstrap.servers", ApplicationConfig.KAFKA_BOOTSTRAP_SERVERS)
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    //开启幂等性
    properties.put("enable.idempotence", (true: java.lang.Boolean))
    var producer: KafkaProducer[String, String] = null
    try
      producer = new KafkaProducer[String, String](properties)
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
    producer
  }

  //发送消息
  def send(topic: String, msg: String): Unit = {
    //单例模式?
    if (null == kafkaProducer) kafkaProducer = createKafkaProducer
    kafkaProducer.send(new ProducerRecord[String, String](topic, msg))
  }

  //发送消息,重载
  def send(topic: String, key: String, msg: String): Unit = {
    //单例模式?
    if (null == kafkaProducer) kafkaProducer = createKafkaProducer
    kafkaProducer.send(new ProducerRecord[String, String](topic, key, msg))
  }
}
