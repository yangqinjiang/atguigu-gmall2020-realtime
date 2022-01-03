package com.atguigu.gmall.realtime.config

import com.typesafe.config.{Config, ConfigFactory}

object ApplicationConfig {

  private val config:Config = ConfigFactory.load("rt-config.properties")
  /*
    Kafka 相关配置信息
  */
  lazy val KAFKA_BOOTSTRAP_SERVERS: String = config.getString("kafka.bootstrap.servers")
  lazy val KAFKA_AUTO_OFFSET_RESET: String = config.getString("kafka.auto.offset.reset")

  /*
  Redis 数据库
*/
  lazy val REDIS_HOST: String = config.getString("redis.host")
  lazy val REDIS_PORT: Int = config.getInt("redis.port")
  lazy val REDIS_DB: Int = config.getInt("redis.db")
  lazy val REDIS_MAX_TOTAL: Int = config.getInt("redis.max_total")

  /**
   * ES
   */
  lazy val ES_NODES:String = config.getString("es.nodes")
  lazy val ES_PORT:String = config.getString("es.port")
  lazy val ES_NODES_PORT:String = s"http://${ES_NODES}:${ES_PORT}"
  lazy val ES_MAX_TOTAL_CONNECTION:Int = config.getInt("es.maxTotalConnection")

  /**
   * Mysql
   */
  lazy val MYSQL_DRIVER:String = config.getString("mysql.driver")
  lazy val MYSQL_URL:String = config.getString("mysql.url")
  lazy val MYSQL_USER:String = config.getString("mysql.user")
  lazy val MYSQL_PASSWORD:String = config.getString("mysql.password")

  /**
   * Phoenix
   */
  lazy val PHOENIX_DRIVER:String = config.getString("phoenix.driver")
  lazy val PHOENIX_CONN:String = config.getString("phoenix.conn")

  /**
   * HBase
   */
  lazy val HBASE_HOST:String = config.getString("hbase.host")
}
