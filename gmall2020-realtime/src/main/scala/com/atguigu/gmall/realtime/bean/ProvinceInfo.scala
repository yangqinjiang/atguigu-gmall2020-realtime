package com.atguigu.gmall.realtime.bean

/**
 * 省份样例类
 */
case class ProvinceInfo(
                       id:String,
                       name:String,
                       area_code:String,
                       iso_code:String,
                       )


object ProvinceInfo{
  //是空对象模式,其中属性值均为空值
  val emptyObj : ProvinceInfo = ProvinceInfo("0","","","")
}