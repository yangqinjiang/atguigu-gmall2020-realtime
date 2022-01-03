package com.atguigu.gmall.realtime.bean

/**
 *   Spu 样例类
 */
case class SpuInfo(
                    id:String ,
                    spu_name:String
                  )
object SpuInfo{
  //是空对象模式,其中属性值均为空值
  val emptyObj :SpuInfo = SpuInfo("0","")
}