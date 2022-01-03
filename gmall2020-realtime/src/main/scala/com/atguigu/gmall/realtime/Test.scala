package com.atguigu.gmall.realtime

import com.alibaba.fastjson.JSONObject
import com.atguigu.gmall.realtime.bean.ProvinceInfo

import scala.collection.mutable.ListBuffer

object Test {
  def main(args: Array[String]): Unit = {
    val buf = new ListBuffer[JSONObject]()
    for (i <- 1 to 10){
      val jsonObj = new JSONObject()
      jsonObj.put("ID",i.toString)
      jsonObj.put("NAME","zs")
      jsonObj.put("AREA_CODE","1000")
      jsonObj.put("ISO_CODE","CN-JX")
      println(jsonObj.toJSONString)
      buf.append(jsonObj)
    }
    val list = buf.toList
    val map: Map[Long, ProvinceInfo] = list.map(obj => {

      val info: ProvinceInfo = obj.toJavaObject(classOf[ProvinceInfo])
      (info.id.toLong, info)
    }).toMap
    println(map)
    println(map.getOrElse(100, ProvinceInfo.emptyObj))

//    val info: ProvinceInfo = jsonObj.toJavaObject(classOf[ProvinceInfo])
//    println(info)
//    println(ProvinceInfo.emptyObj)
//
//    println("0".toLong)
//    println(SkuInfo.emptyObj)

//    println(BaseCategory3.emptyObj.asInstanceOf[BaseCategory3])
  }
}
