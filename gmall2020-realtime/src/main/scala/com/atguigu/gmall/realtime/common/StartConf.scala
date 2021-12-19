package com.atguigu.gmall.realtime.common

import org.apache.spark.streaming.Duration

/**
 * 启动参数,
 * @param master spark master
 * @param topic  kafka 主题
 * @param groupId kafka 消费组
 * @param batchDuration spark Streaming 的周期时长
 */
case class StartConf(val master: String = "local[*]"
                     , val topic: String = "RTTopic"
                     , val groupId: String = "RTGroupId",
                     val batchDuration: Duration)
