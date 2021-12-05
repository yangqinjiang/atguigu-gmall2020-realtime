package com.atguigu.gmall.realtime.utils


import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.util.Properties

/**
 * 加载指定的配置文件
 */
object MyPropertiesUtil {

  def load(propertiesName:String):Properties = {
    val prop:Properties = new Properties()
    //加载指定的配置文件
    prop.load(new InputStreamReader(
      Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName),
      StandardCharsets.UTF_8
    ))
    prop
  }

  def main(args: Array[String]): Unit = {
    val prop: Properties = MyPropertiesUtil.load("config.properties")
    println(prop.getProperty("kafka.broker.list"))
  }
}
