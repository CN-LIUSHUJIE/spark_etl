package com.pingan.core.scala

import org.apache.spark.{SparkConf, SparkContext}

object Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("local")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val books=List("Hadoop","Hive","HDFS")

    val A1=books.map{a=>a.toUpperCase()}

    val B1=books.flatMap(a=>a.toUpperCase().split("H"))

    println(A1+" " + B1)

    val str:Double = "20.01".toDouble
    println(str)
  }
}
