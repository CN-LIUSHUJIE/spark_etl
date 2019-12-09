package com.pingan.streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Top3HotProduct {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Top3HotProduct")
    val ssc = new StreamingContext(conf,Seconds(1))

    val productClickLogsDStream: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.56.128",9999)
    val categoryProductPairsDStream = productClickLogsDStream
      .map{productClickLog =>
        (productClickLog.split(" ")(2)+"_"+productClickLog.split(" ")(1),1)}

   val categoryProductCountsDStream =  categoryProductPairsDStream.reduceByKeyAndWindow(
      (v1:Int,v2:Int) => v1+v2,Seconds(60),Seconds(10))

    categoryProductCountsDStream.foreachRDD(foreachFunc = categoryProductCountsRDD => {
      val categoryProductCountRowRDD = categoryProductCountsRDD.map(f = tuple => {
        val category = tuple._1.split("_")(0)
        val product = tuple._1.split("_")(1)
        val count = tuple._2
        Row(category, product, count)
      })

      val structType = StructType(Array(
        StructField("category", StringType, true),
        StructField("product", StringType, true),
        StructField("count", IntegerType, true)))

      val hiveContext = new HiveContext(categoryProductCountsRDD.context)
      val categoryProductCountDF: DataFrame = hiveContext.createDataFrame(categoryProductCountRowRDD, structType)
      categoryProductCountDF.registerTempTable("product_click_log")

      val top3ProductDF = hiveContext.sql(
        "select category,product,count "
        + " from ("
          + "select "
          + "category,"
          + "product,"
          + "count,"
          + "row_number() over (partition by category order by count desc ) rank"
        + "from product_click_log"
        + ") tmp"
        + "where rank <= 3")
      top3ProductDF.show()
    })



   ssc.start()
   ssc.awaitTermination()
  }
}
