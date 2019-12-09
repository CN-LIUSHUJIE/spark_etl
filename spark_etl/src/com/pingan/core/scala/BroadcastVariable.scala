package com.pingan.core.scala

import org.apache.spark.{SparkConf, SparkContext}

object BroadcastVariable {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("BroadcastVariable")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val factor = 3

    val factorBroadcast = sc.broadcast(factor)

    val numberArray = Array(1,2,3,4,5,6,7,8,9,10)

    val numbers = sc.parallelize(numberArray)

    val multipleNumbers = numbers.map(number => number * factorBroadcast.value)

    multipleNumbers.foreach(num => println(num))

    sc.stop()



  }
}
