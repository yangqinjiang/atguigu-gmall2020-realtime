package com.atguigu.gmall.realtime.bean

/**
 *  品牌样例类
 * @param tm_id
 * @param tm_name
 */
case class BaseTrademark(
                        tm_id:String,
                        tm_name:String
                        )

object BaseTrademark{
  //空对象模式,其中属性值均为空值
  val emptyObj :BaseTrademark = BaseTrademark("0","")
}
