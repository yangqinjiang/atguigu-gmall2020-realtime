package com.atguigu.gmall.realtime.bean

/**
 *  分类样例类
 */
case class BaseCategory3(
                          id:String ,
                          name:String ,
                          category2_id:String
                        )

object BaseCategory3{
  //空对象模式,其中属性值均为空值
  val emptyObj:BaseCategory3 = BaseCategory3("0","","")
}
