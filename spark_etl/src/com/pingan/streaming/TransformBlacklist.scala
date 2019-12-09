package com.pingan.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * transform以及广告计费日志实时黑名单过滤
  */
object TransformBlacklist {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("TransformBlacklist")

    val ssc = new StreamingContext(conf,Seconds(5))

    val blacklist = Array(("tom",true))

    val blacklistRDD = ssc.sparkContext.parallelize(blacklist,5)
    val adsClickLogDStream = ssc.socketTextStream("hadoop",9999)

    val userAdsClickLogDStream = adsClickLogDStream.map(adsClickLog =>
      (adsClickLog.split(" ")(1),adsClickLog)
    )

    val valiAdsClickLogDStream  = userAdsClickLogDStream.transform(userAdsClickLogRDD =>{

      val joinRDD = userAdsClickLogRDD.leftOuterJoin(blacklistRDD)

      val filteredRDD = joinRDD.filter(tuple =>{
        if(tuple._2._2.getOrElse(false)){
          false
        }
          true
      })

      val validAdsClickLogRDD = filteredRDD.map(tuple => tuple._2._1)
      validAdsClickLogRDD

    })

    valiAdsClickLogDStream.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
