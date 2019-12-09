package com.pingan.workcode

import org.apache.spark.sql.hive.HiveContext

object ReportAccoutAgeVintage {
  def createReportAccountAgeVintageJob(hiveContext:HiveContext,yesterdayyyy_MM_dd:String): Unit ={

    import hiveContext.sql

    sql("use jk_daikuan_safe")

    /**
      * 创建账龄表report_account_age_vintage
      */

    val createReportAccountAgeVintage =
      """
        |create table if not exists report_account_age_vintage(
        |snap_date              string,
        |loan_no                string,
        |fund_source            string,
        |loan_amt               decimal(17,2),
        |overduedays            int
        |)
        |ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n' stored textfile
      """.stripMargin
    sql(createReportAccountAgeVintage)

    val createReportAccountAgeVintagePartition =
      """
        |create table if not exists report_account_age_vintage_partition(
        |snap_date              string,
        |loan_no                string,
        |fund_source            string,
        |loan_amt               decimal(17,2),
        |overduedays            int
        |) PARTITIONED BY (dt string)
        |ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n' stored textfile
      """.stripMargin

    sql(createReportAccountAgeVintagePartition)

    val insertReportAccountAgeVintage =
      """
        |insert overwrite table report_account_age_vintage
        |select
        |snap_date,
        |loan_no,
        |fund_source,
        |loan_amt,
        |overduedays
        |from ploan_order
      """.stripMargin

    sql(insertReportAccountAgeVintage)

    val insertReportAccountAgeVintagePartition =
      s"""
        |insert overwrite table report_account_age_vintage_partition partition(dt="$yesterdayyyy_MM_dd")
        |select
        |snap_date,
        |loan_no,
        |fund_source,
        |loan_amt,
        |overduedays
        |from
        |report_account_age_vintage
      """.stripMargin

    sql(insertReportAccountAgeVintagePartition)

  }

}
