package com.pingan.utils






import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat


/**
  * @author liushujie
  *         2018/9/9 19:26
  */
object DataUtils {

  val YYYYMMDDHHMMSS_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  val TAG_FORMAT = FastDateFormat.getInstance("yyyyMMdd")

  /**
    * 把当前时间转换成时间戳
    * @param time
    * @return
    */
  def getTime(time:String) ={
    YYYYMMDDHHMMSS_FORMAT.parse(time).getTime
  }

  def parseToMin(time:String) ={
    TAG_FORMAT.format(new Date(getTime(time)))
  }


  def main(args: Array[String]): Unit = {
    print(parseToMin("2017-11-20 00:39:26"))
  }





}
