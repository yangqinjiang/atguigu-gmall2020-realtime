package com.atguigu.gmall.realtime.common

/**
 * 启动参数
 * @param topic  kafka 主题
 * @param groupId kafka 消费组
 * @param seconds spark Streaming 的周期时长,默认为5s
 */
case class StartConf(val topic: String = "RTTopic"
                     , val groupId: String = "RTGroupId",
                     val seconds: Long = 5)
