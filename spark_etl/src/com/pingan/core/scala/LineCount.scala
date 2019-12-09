package com.pingan.core.scala

import org.apache.spark.{SparkConf, SparkContext}

object LineCount {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("LineCount")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val lines =  sc.textFile("D://test//words.txt",2)

    val pairs = lines.map(line => (line,1))

    val lineCounts = pairs.reduceByKey(_+_)

    lineCounts.foreach(line =>

    println(line._1+" apprears "+ line._2+" times")

    )

    sc.stop()
  }
}
