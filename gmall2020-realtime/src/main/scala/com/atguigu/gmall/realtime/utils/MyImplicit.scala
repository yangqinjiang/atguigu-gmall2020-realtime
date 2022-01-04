package com.atguigu.gmall.realtime.utils

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.dstream.DStream

import scala.language.implicitConversions
import scala.reflect.{ClassTag, classTag}

object MyImplicit extends Logging{
  //隐式转换
  implicit def transformToObj[T: ClassTag](ds: DStream[ConsumerRecord[String, String]]): DStream[T] = {
    val objectDStream: DStream[T] = ds.map {
      record => { //引入反射
        logWarning("implicit transformToObj, class type ="+classTag[T].runtimeClass)
        val t: T = JSON.parseObject(record.value(), classTag[T].runtimeClass)
        t
      }
    }
    objectDStream
  }
}
