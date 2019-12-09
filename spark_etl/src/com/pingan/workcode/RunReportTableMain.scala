package com.pingan.workcode

import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}
import java.util.{Calendar, Date}

import org.apache.spark.sql.hive.HiveContext

object RunReportTableMain {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()

    val sc = new SparkContext(conf)

    val hiveContext = new HiveContext(sc)

    val now = new Date()

    val dateFormat1 = new SimpleDateFormat("yyyy-MM-dd")

    val dateFormat2 = new SimpleDateFormat("yyyyMMdd")

    val cal = Calendar.getInstance()

    cal.add(Calendar.DATE,-1)

    val yesterday_mm_dd = dateFormat1.format(cal.getTime())

    val yesterdayyyyMMdd = dateFormat2.format(cal.getTime())

    /**
      * 账龄表
      */
    ReportAccoutAgeVintage.createReportAccountAgeVintageJob(hiveContext:HiveContext,yesterdayyyyMMdd:String)


    sc.stop()

  }
}
