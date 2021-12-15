package com.atguigu.gmall.realtime.bean

/**
 * 商品sku样例类
 */
case class SkuInfo(
                    id:String ,
                    spu_id:String ,
                    price:String ,
                    sku_name:String ,
                    tm_id:String ,
                  //关联字段??
                    category3_id:String ,
                    create_time:String,
                    var category3_name:String,
                    var spu_name:String,
                    var tm_name:String
                  )
