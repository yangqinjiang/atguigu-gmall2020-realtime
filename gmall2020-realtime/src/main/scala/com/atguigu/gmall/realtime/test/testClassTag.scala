package com.atguigu.gmall.realtime.test

import scala.reflect.{ClassTag, classTag}

object testClassTag {
  def main(args: Array[String]): Unit = {
    val arr = createArray[Int](1, 2)
    println(arr.mkString(","))
    val arr2 = transformToObj[Int](1, 2)
    println(arr2.mkString(","))
  }

  implicit def transformToObj[T: ClassTag](one: T, two: T) = {
    // 创建以一个数组 并赋值
    val arr = new Array[T](2) //期望根据T的类型动态生成不同数据类型的数组
    arr(0) = one
    arr(1) = two
    println(classTag[T].runtimeClass)
    arr
  }

  def createArray[T: ClassTag](one: T, two: T) = {
    // 创建以一个数组 并赋值
    val arr = new Array[T](2) //期望根据T的类型动态生成不同数据类型的数组
    arr(0) = one
    arr(1) = two
    arr
  }
}
