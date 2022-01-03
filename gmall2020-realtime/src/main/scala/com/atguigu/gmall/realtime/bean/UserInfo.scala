package com.atguigu.gmall.realtime.bean

/**
 * 用户样例类
 */
case class UserInfo(
                   id:String,
                   user_level:String,
                   birthday:String,
                   gender:String,
                   var age_group:String,//年龄段
                   var gender_name:String//性别
                   )

object  UserInfo{
  //是空对象模式,其中属性值均为空值
  val emptyObj :UserInfo = UserInfo("0","0","","","","")
}
