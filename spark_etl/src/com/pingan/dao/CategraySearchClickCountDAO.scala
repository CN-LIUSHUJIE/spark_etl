package com.pingan.dao

import com.pingan.domain.CategarySearchClickCount
import com.pingan.utils.HbaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * @author liushujie
  *         2018/9/10 15:46
  */
object CategraySearchClickCountDAO {
  val tableName = "categray_search_count"
  val cf = "info"
  val qualifer = "click_count"

  /**
    * 保存数据
    * @param list
    */
  def save(list:ListBuffer[CategarySearchClickCount]): Unit ={
    val table =  HbaseUtils.getInstance().getHtable(tableName)
    for(els <- list){
      table.incrementColumnValue(Bytes.toBytes(els.day_search_categray),Bytes.toBytes(cf),Bytes.toBytes(qualifer),els.clickCount)
    }
  }

  def count(day_categary:String) : Long={
    val table  =HbaseUtils.getInstance().getHtable(tableName)
    val get = new Get(Bytes.toBytes(day_categary))
    val value =  table.get(get).getValue(Bytes.toBytes(cf), Bytes.toBytes(qualifer))
    if(value == null){
      0L
    }else{
      Bytes.toLong(value)
    }
  }

  def main(args: Array[String]): Unit = {
    val list = new ListBuffer[CategarySearchClickCount]
    list.append(CategarySearchClickCount("20171122_1_1",300))
    //list.append(CategaryClickCount("20171122_9", 60))
    //list.append(CategaryClickCount("20171122_10", 160))
    save(list)
//
    print(count("20171122_1_1")+"---")

  }
}
