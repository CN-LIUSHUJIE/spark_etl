package com.pingan.core.scala

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //创建spark配置，设置应用程序名字
          val conf = new SparkConf()
            .setAppName("WordCount")
              .setMaster("local[2]")
    //创建spark执行的入口
    val sc = new SparkContext(conf)
    val lines = sc.textFile("hdfs://192.168.32.128:8020/words.txt")
    //切分压平
    val words = lines.flatMap({
      line => line.split(" ")
    })
    //将单词和一组合
    val pairs = words.map{ word => (word,1)}
    //按key进行聚合
    val wordCounts = pairs.reduceByKey{ _+_ }
    val sorted = wordCounts.sortBy(_._2,false)
    sorted.foreach(wordCount => println(wordCount._1+" appeared "+ wordCount._2+ " times"))
    sc.stop()
  }
}
