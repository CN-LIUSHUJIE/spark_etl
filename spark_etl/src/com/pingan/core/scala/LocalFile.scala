package com.pingan.core.scala

import org.apache.spark.{SparkConf, SparkContext}

object LocalFile {
  def main(args: Array[String]): Unit = {

    val  conf = new SparkConf()
              .setAppName("LocalFile")
              .setMaster("local")

    val sc = new SparkContext(conf)

    val files = sc.textFile("D:\\test\\words.txt")

   val count =  files.map(file => {
     file.length()
   })

    val sum = count.reduce(_+_)

    println("sum:"+sum)

    sc.stop()
  }
}
