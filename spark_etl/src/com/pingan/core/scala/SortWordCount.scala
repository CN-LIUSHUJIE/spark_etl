package com.pingan.core.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SortWordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("SortWordCount")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("D://test//words.txt")

    val words = lines.flatMap(line => line.split(" "))

    val pairs = words.map(word => (word,1))

    val wordCounts = pairs.reduceByKey(_+_)

    val countWords: RDD[(Int, String)] = wordCounts.map(wordCount => (wordCount._2,wordCount._1))

    val sortCountWords = countWords.sortByKey(false)

    val sortWordsCount: RDD[(String, Int)] = sortCountWords.map(sortCountWord =>
      (sortCountWord._2,sortCountWord._1)
    )

    sortWordsCount.foreach(words =>
      println("单词: "+words._1+" 出现 "+ words._2+" 次")
    )

    sc.stop()
  }
}
