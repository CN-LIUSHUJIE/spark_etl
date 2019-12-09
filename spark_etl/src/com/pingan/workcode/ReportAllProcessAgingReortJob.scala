package com.pingan.workcode

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable.ListBuffer

/**
  * Created by EX-LIUSHUJIE001 on 2018/07/13
  * 全流程表
  */

object ReportAllProcessAgingReortJob {

  def createDapProcessAgingReportJob(hiveContext: HiveContext){

    import hiveContext.sql

    sql("use jk_daikuan_safe")

    val data1 =
      """
        |select
        |id_order,
        |name
        |from
        |ploan_order
      """.stripMargin
    sql(data1).registerTempTable("ploan_order_tmp")

    val ploan_order_tmp =
      """
        |select
        |id_order,
        |name
        |from
        |ploan_order_tmp
      """.stripMargin

    val df = sql(ploan_order_tmp)

    val data2 = df.map(row =>{
      val id_order = row.getAs[String]("id_order")
      val name = row.getAs[String]("name")

      val list:ListBuffer[Any] = ListBuffer()
      list.+=(id_order)
      list.+=(name)
      Row.fromSeq(list.seq)
    })

    val schema = StructType(
      StructField("id_order",StringType) ::
      StructField("name",StringType) ::
      Nil
    )

    val newDf: DataFrame = df.sqlContext.createDataFrame(data2,schema)

    newDf.registerTempTable("dap_process_aging_report_tmp")

    sql("drop table if exists dap_process_aging_report")

    sql("create table dap_process_aging_report as select * from dap_process_aging_report_tmp")

  }
}
