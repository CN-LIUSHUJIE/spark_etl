package com.pingan.core.scala

import org.apache.spark.{SparkConf, SparkContext}

object ParallelizeCollection {
  def main(args: Array[String]): Unit = {
    val  conf = new SparkConf()
        .setAppName("ParallelizeCollection")
      .setMaster("local")

    val sc =  new SparkContext(conf)

    val lists = List(1,2,3,4,5,6,7,8,9,10)

    /**
      * 给这个RDD指定5个分区
      */

    val numbers = sc.parallelize(lists,5)

    val  sum = numbers.reduce(_+_)

    println("sum:"+sum)
  }
}
