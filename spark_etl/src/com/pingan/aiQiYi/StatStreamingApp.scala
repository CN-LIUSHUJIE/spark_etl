package com.pingan.aiQiYi

import com.pingan.dao.{CategaryClickCountDAO, CategraySearchClickCountDAO}
import com.pingan.domain.{CategaryClickCount, CategarySearchClickCount, ClickLog}
import com.pingan.utils.DataUtils
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.collection.mutable.ListBuffer




/**
  * @author liushujie
  *         2018/9/7 22:36
  */
object StatStreamingApp {
  val args = Array("hadoop:9092","test-consumer-group","testFlume","1")
  def main(args: Array[String]) {


   val sprakConf = new SparkConf().setAppName("DirectKafkaWordCountDemo")
    //此处在idea中运行时请保证local[2]核心数大于2
    sprakConf.setMaster("local[2]")
    val ssc = new StreamingContext(sprakConf, Seconds(5))

    val brokers = "hadoop:9092";

    //val topics = "testFlume2";
    val topics = "flinkTopic"

    val topicSet = topics.split(",").toSet

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    val logs: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)

    val cleanLog = logs.map(_._2).map(line=>{
       var infos = line.split("\t")
       var url: String = infos(2).split(" ")(1)
       var categrayId = 0
       if(url.startsWith("www")){
        categrayId = url.split("/")(1).toInt
       }
       ClickLog(infos(0),DataUtils.parseToMin(infos(1)),categrayId,infos(3),infos(4).toInt)
     }).filter(log=>log.categrayId!=0)

    cleanLog.print()


    /**
      * 保存数据到hbase 每个栏目的点击量
      */
    cleanLog.map(log=>{
      (log.time.substring(0,8)+log.categrayId,1)
    }).reduceByKey(_+_).foreachRDD(rdd=>{
      rdd.foreachPartition(partitions=>{
        val list = new ListBuffer[CategaryClickCount]
        partitions.foreach(pair=>{
          list.append(CategaryClickCount(pair._1,pair._2))
        })
        CategaryClickCountDAO.save(list)
      })
    })
    println("-===============-")
    cleanLog.map(log=>{
      print("ccccc"+CategaryClickCountDAO.count(log.time.substring(0,8)+log.categrayId))
    })

    /**
      * 保存数据  每个网址的每个栏目的点击量
      */
    cleanLog.map(log=>{
          val url = log.refer.replace("//","/")
          val splits = url.split("/")
          var host = ""
          if(splits.length > 2){
             host= splits(1)
          }
          (host,log.time,log.categrayId)
      }).filter(x=>x._1 != "")
        .map(x=>{
            (x._2.substring(0,8)+"_"+x._1+"_"+x._3,1)
      }).reduceByKey(_+_)
        .foreachRDD(rdd=>{rdd.foreachPartition(partitions=>{
               val list = new ListBuffer[CategarySearchClickCount]
                partitions.foreach(pairs=>{
                  list.append(CategarySearchClickCount(pairs._1,pairs._2))
               })
               CategraySearchClickCountDAO.save(list)
       })
      })

    ssc.start()
    ssc.awaitTermination()
  }

//main  psvm

}
