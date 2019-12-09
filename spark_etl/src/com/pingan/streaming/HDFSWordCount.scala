package com.pingan.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object HDFSWordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("HDFSWordCount")
      .setMaster("local[2]")

    val ssc = new StreamingContext(conf,Seconds(5))

    val lines = ssc.textFileStream("hdfs:hadoop:9000/wordcount_dir")

    val words = lines.map{line => line.split(" ")}

    val pairs = words.map(word => (word,1))

    val wordCountS = pairs.reduceByKey(_+_)

    wordCountS.print()

    ssc.start()

    ssc.awaitTermination()
  }
}
