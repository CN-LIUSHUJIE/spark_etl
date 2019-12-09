package com.pingan.core.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 获取文本内最大的3个数字
  */
object Top3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Top3")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("D://test//top.txt")

    val mapLines = lines.map(line => (line.toInt,line))

    val sortNums: RDD[(Int, String)] = mapLines.sortByKey(false)

    val mapNums = sortNums.map(num => num._1)

    val top3  = mapNums.take(3)

    for(num <- top3){
      println(num)
    }

    sc.stop()
  }
}
