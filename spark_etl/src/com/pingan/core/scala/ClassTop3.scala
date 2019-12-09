package com.pingan.core.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 每个班级前三名
  */
object ClassTop3 {
  def main(args: Array[String]): Unit = {
    val conf = new  SparkConf()
      .setAppName("ClassTop3")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("D://test//ClassTop.txt")

    lines.map { line =>
      val info =line.split(" ")
      (info(0),info(1).toInt)
    }.groupByKey().map{ m=>
      val className = m._1
      val top3 = m._2.toArray.sortWith(_>_).take(3)
      (className,top3)
    }.foreach{item=>
      val className = item._1
      println("班级: "+ className + "的前3名是: ")
      item._2.foreach(m=>{
        println(m)
      })
    }

    sc.stop()

  }
}
