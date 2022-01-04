package com.atguigu.gmall.realtime.utils

import org.apache.kafka.common.TopicPartition
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import java.util

/**
 * 偏移量管理类，用于读取和保存偏移量
 */
object OffsetManagerUtil  extends Logging{
  /**
   * 从 Redis 中读取偏移量
   * Reids 格式： type=>Hash [key=>offset:topic:groupId field=>partitionId
value=>偏移量值] expire 不需要指定
   * @param topicName 主题名称
   * @param groupId 消费者组
   * @return 当前消费者组中，消费的主题对应的分区的偏移量信息
   * KafkaUtils.createDirectStream 在读取数据的时候封装了
Map[TopicPartition,Long]
   */
  def getOffset(topicName:String,groupId:String):Map[TopicPartition,Long] = {

    //获取redis客户端
    val jedis: Jedis = MyRedisUtil.getJedisClient
    //拼装Redis中存储偏移量的key
    val offsetKey:String = "offset:" + topicName +":"+groupId
    //根据key从Redis中获取数据
    val offsetMap: util.Map[String, String] = jedis.hgetAll(offsetKey)
    //关闭客户端
    jedis.close()
    //将java的Map转换为Scala的Map,方便后续操作
    import scala.collection.JavaConverters._
    val kafkaOffsetMap: Map[TopicPartition, Long] = offsetMap.asScala.map {
      case (partitionId, offset) => {
        logWarning("读取分区偏移量: " + partitionId + ":" + offset)
        //将Redis中保存的分区对应的偏移量进行封装
        (new TopicPartition(topicName, partitionId.toInt), offset.toLong)
      }
    }.toMap
    kafkaOffsetMap
  }
  /**
   * 向 Redis 中保存偏移量
   * Reids 格式： type=>Hash [key=>offset:topic:groupId field=>partitionId value=>
偏移量值] expire 不需要指定
   *
   * @param topicName 主题名
   * @param groupId 消费者组
   * @param offsetRanges 当前消费者组中，消费的主题对应的分区的偏移量起始和结束信息
   */
  def saveOffset(topicName:String,groupId:String,offsetRanges:Array[OffsetRange]):Unit = {
    //定义java的map集合, 用于向redis中保存数据
    val offsetMap: util.HashMap[String, String] = new util.HashMap[String, String]()
    //对封装的偏移量数组offsetRanges进行遍历
    for (offset <- offsetRanges) {
      //获取分区
      val partition: Int = offset.partition
      //获取结束点
      val untilOffset: Long = offset.untilOffset
      //封装到Map集合中
      offsetMap.put(partition.toString,untilOffset.toString)
      //打印测试
      if(offset.untilOffset > offset.fromOffset){
        logWarning("保存分区: "+partition+":"+offset.fromOffset+"---->"+offset.untilOffset)
      }
    }
    //TODO: 如果数据没有更新,则不执行下面的操作
    //如果需要保存的偏移量不为空, 执行保存操作
    if (offsetMap!= null&&offsetMap.size() > 0){
      //获取Redis客户端
      val jedis: Jedis = MyRedisUtil.getJedisClient
      //拼接Redis中存储偏移量的key
      val offsetKey = "offset:" + topicName + ":" + groupId
      //保存到redis中
      jedis.hmset(offsetKey,offsetMap)
      //关闭客户端
      jedis.close()
    }
  }
}
