package com.atguigu.gmall.realtime.utils


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.streaming.StreamingContext

/**
 * 当应用启动以后，循环判断 HDFS上目录下某个文件（监控文件）是否存在，如果存在就优雅停止应用
 * -a). 启动流式应用时创建目录
 * ${HADOOP_HOME}/bin/hdfs dfs -mkdir -p /spark/streaming
 * -b). 关闭应用时，HDFS创建文件stop
 * ${HADOOP_HOME}/bin/hdfs dfs -touchz /spark/streaming/stop
 * -c). 启动应用时，删除文件stop
 * ${HADOOP_HOME}/bin/hdfs dfs -rm -R /spark/streaming/stop
 */
object StreamingUtils {

  /**
   * 判断是否存在 mark file
   *
   * @param monitorFile Monitor 文件
   */
  def isExistsMonitorFile(monitorFile: String, hadoopConfiguration: Configuration): Boolean = {
    val monitorPath = new Path(monitorFile)
    val fs = monitorPath.getFileSystem(hadoopConfiguration)
    fs.exists(monitorPath)
  }

  /**
   * 当应用启动以后，循环判断 HDFS上目录下某个文件（监控文件）是否存在，如果存在就优雅停止应用
   * -a). 启动流式应用时创建目录
   * ${HADOOP_HOME}/bin/hdfs dfs -mkdir -p /spark/streaming
   * -b). 关闭应用时，HDFS创建文件stop
   * ${HADOOP_HOME}/bin/hdfs dfs -touchz /spark/streaming/stop
   * -c). 启动应用时，删除文件stop
   * ${HADOOP_HOME}/bin/hdfs dfs -rm -R /spark/streaming/stop
   */
  def stopStreaming(ssc: StreamingContext, monitorFile: String): Unit = {
    //每隔10s中检查应用是否停止
    val checkInterval = 10 * 1000
    //应用是否停止
    var isStreamingStop = false
    //循环判断 HDFS上目录下某个文件(监控文件)是否存在, 如果存在就停止 优雅停止应用
    while (!isStreamingStop) {
      isStreamingStop = ssc.awaitTerminationOrTimeout(checkInterval)

      //TODO: 使用Redis保存这些状态
      val isExists = isExistsMonitorFile(
        monitorFile, ssc.sparkContext.hadoopConfiguration
      )
      if (!isStreamingStop && isExists) {
        ssc.stop(stopSparkContext = true, stopGracefully = true)
      }
    }
  }

  /**
   * 当应用启动以后，循环判断 HDFS上目录下某个文件（监控文件）是否存在，如果存在就优雅停止应用
   * -a). 启动流式应用时创建目录
   * ${HADOOP_HOME}/bin/hdfs dfs -mkdir -p /spark/order-apps/stop
   * -b). 关闭应用时，HDFS创建文件stop
   * ${HADOOP_HOME}/bin/hdfs dfs -touchz /spark/order-apps/stop/etl-stop
   * -c). 启动应用时，删除文件stop
   * ${HADOOP_HOME}/bin/hdfs dfs -rm -R /spark/order-apps/stop/etl-stop
   */
  def stopStructuredStreaming(query: StreamingQuery, monitorFile: String): Unit = {
    //每隔10s中检查应用是否停止
    val checkInterval = 10 * 1000
    //应用是否停止
    var isStreamingStop = false
    //循环判断 HDFS上目录下某个文件(监控文件)是否存在, 如果存在就停止 优雅停止应用
    while (!isStreamingStop) {
      isStreamingStop = query.awaitTermination(checkInterval)
      //TODO: 使用Redis保存这些状态
      val isExists =  isExistsMonitorFile(
        monitorFile, query.sparkSession.sparkContext.hadoopConfiguration
      )
      if (!isStreamingStop && isExists) {
        query.stop() //停止查询
      }
    }
  }
}
