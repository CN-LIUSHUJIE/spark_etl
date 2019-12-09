package com.pingan.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WindowHotWord {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("WindowHotWord")
    val ssc = new StreamingContext(conf,Seconds(1))

    val searchLogsDStream  = ssc.socketTextStream("hadoop",9999)

    val searchWordsDStream  = searchLogsDStream.map(_.split(" ")(1))

    val searchWordPairsDStream = searchWordsDStream.map{searchWord =>(searchWord,1)}

   val searchWordCountsDStream: DStream[(String, Int)] =  searchWordPairsDStream.reduceByKeyAndWindow(
     (v1:Int,v2:Int)=>v1+v2,
      Seconds(60),
      Seconds(10))

   val finalDStream =  searchWordCountsDStream.transform(searchWordCountsRDD=> {
      val countSearchWordsRDD = searchWordCountsRDD.map(tuple => (tuple._2, tuple._1))
      val sortedCountSearchWordsRDD = countSearchWordsRDD.sortByKey(false)


      val sortedSearchWordCountsRDD = sortedCountSearchWordsRDD.map(sortedCountSearchWords=> (sortedCountSearchWords._2,sortedCountSearchWords._1))
      val top3SearchWordCounts: Array[(String, Int)] = sortedSearchWordCountsRDD.take(3)

      for (tuple <- top3SearchWordCounts){
        println(tuple)
      }
      searchWordCountsRDD
    })
    finalDStream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
