package com.pingan.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * updateStateByKey以及基于缓存机制的wordCount
  */
object UpdateStateByKeyWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("UpdateStateByKeyWordCount")

    val ssc = new StreamingContext(conf,Seconds(5))

    ssc.checkpoint("hdfs://hadoop:8020/wordcount_checkpoint")

    val lines = ssc.socketTextStream("hadoop",9999)

    val words = lines.flatMap(_.split(" "))

    val pairs = words.map{word =>(word,1)}

    val wordCounts = pairs.updateStateByKey((values:Seq[Int],state:Option[Int])=>{
      var newValue = state.getOrElse(0)
      for (value <- values){
        newValue += value
      }
      Option(newValue)
    })

    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
