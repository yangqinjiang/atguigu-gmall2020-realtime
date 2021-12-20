package com.atguigu.gmall.realtime.utils

import com.alibaba.fastjson.JSONObject
import com.atguigu.gmall.realtime.config.ApplicationConfig

import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData, Statement}
import scala.collection.mutable.ListBuffer

/**
 * 查询phoenix工具类
 */
object PhoenixUtil {

  def queryList(sql: String): List[JSONObject] = {
    Class.forName(ApplicationConfig.PHOENIX_DRIVER)
    //返回的结果集合
    val resultList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]()
    val conn: Connection = DriverManager.getConnection(ApplicationConfig.PHOENIX_CONN)
    val stat: Statement = conn.createStatement()
    //    println(sql)
    val rs: ResultSet = stat.executeQuery(sql)
    val md: ResultSetMetaData = rs.getMetaData // 元数据
    while (rs.next()) { //迭代
      val rowData: JSONObject = new JSONObject()
      for (i <- 1 to md.getColumnCount) {
        //根据元数据,遍历, 组装数据
        rowData.put(md.getColumnName(i), rs.getObject(i))
      }
      resultList += rowData
    }
    //依次关闭
    stat.close()
    conn.close()

    resultList.toList
  }

  def main(args: Array[String]): Unit = {
    val list: List[JSONObject] = queryList("select * from student")
  }
}
