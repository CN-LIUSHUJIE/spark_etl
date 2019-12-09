package com.pingan.core.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TransformationOperation {

  def main(args: Array[String]): Unit = {
    /**
     *将集合中的元素乘以2
     */
    map()

    /**
      *过滤掉集合中偶数的元素
      */
    filter()

    /**
      * 将文本按指定分隔符拆分
      */

    /**
      * 将文本按指定的分割符切割
      */
    flatMap()

    /**
      * 按班级进行分组
      */
    groupBykey()

    /**
      *按照班级对成绩进行聚合
      */
    reduceByKey()

    /**
      * 按照班级对成绩排序(降序)
      */
    sortByKey()

    /**
      * 打印每个学生的成绩
      * */
    joinStudentScore()

    /**
      *
      */
    cogroup()
  }

  def map(){
    val conf = new SparkConf()
      .setAppName("TransformationOperation_Map")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val numbers: RDD[Int] = sc.parallelize(Array(1,2,3,4,5))

    val numbersMap = numbers.map{number => number * 2}

    numbersMap.foreach{numbers => println(numbers)}

    sc.stop()
  }

  def filter(): Unit ={
    val conf = new SparkConf()
      .setAppName("filter")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val numbers = Array(1,2,3,4,5,6,7,8,9,10)

    val numbersRDD = sc.parallelize(numbers)

    val numberFilter = numbersRDD.filter{number => number%2==0}

    numberFilter.foreach{number=>println(number)}

    sc.stop()
  }

  def flatMap(){
    val conf =  new SparkConf()
      .setAppName("flatMap")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val lines = List("运筹学基础 管理经济学 数据结构导论","计算机信息管理毕业设计 网络经济与企业管理 C++程序设计")

    val linesRDD = sc.parallelize(lines)

    val linesFlatMapRDD = linesRDD.flatMap{line=>line.split(" ")}

    linesFlatMapRDD.foreach{line => println(line)}

    sc.stop()
  }

  def groupBykey(): Unit ={
    val conf = new SparkConf()
      .setAppName("groupBykey")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val scoreList: Array[(String, Int)] = Array(Tuple2("class3",91),Tuple2("class1",98),Tuple2("class1",97),Tuple2("class1",90),Tuple2("class2",100),Tuple2("class1",82),Tuple2("class2",89))

    val scores = sc.parallelize(scoreList,1)

    val groupedScores = scores.groupByKey()

    groupedScores.foreach(score => {

      println("class:"+score._1)

      score._2.foreach{singleScore => println(singleScore)}

      println("=================================")
    })
    sc.stop()
  }

  def reduceByKey(): Unit ={
    val conf = new SparkConf()
      .setAppName("reduceBykey")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val scoreList = Array(Tuple2("class1",90),Tuple2("class2",88),Tuple2("class2",99),Tuple2("class3",100))

    val totalSocres = sc.parallelize(scoreList,1)

    val pairtotalSocres = totalSocres.reduceByKey(_+_)

    pairtotalSocres.foreach(socre => println(socre._1+" "+socre._2))

    sc.stop()
  }

  def sortByKey(): Unit ={
    val conf = new SparkConf()
      .setAppName("reduceByKey")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val scoreList: Array[(Int, String)] = Array(Tuple2(91,"Tom"),Tuple2(99,"Jack"),Tuple2(98,"Mary"),Tuple2(92,"Lucy"),Tuple2(88,"Rose"))

    val scoreParallelize = sc.parallelize(scoreList,1)

    val sortByKey = scoreParallelize.sortByKey(false)//默认是true 是升序  false默认是降序

    sortByKey.foreach(socre => println(socre._1+" "+socre._2))

    sc.stop()

  }

  def joinStudentScore(): Unit ={
    val conf = new SparkConf()
      .setAppName("joinStudentScore")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val studentList = Array(Tuple2(1,"Tom"),Tuple2(2,"Jack"),Tuple2(3,"Rose"))

    val scoreList = Array(Tuple2(1,98),Tuple2(2,99),Tuple2(3,100))

    val studentList1 = sc.parallelize(studentList)

    val scoreList1 = sc.parallelize(scoreList)

    val studentScore = studentList1.join(scoreList1)

    studentScore.foreach{row =>
      println("学生编号:"+row._1)
      println("学生姓名:"+row._2._1)
      println("学生成绩:"+row._2._2)
      println("============================")

    }

    sc.stop()
  }

  def cogroup(): Unit ={

        val conf = new SparkConf()
          .setAppName("joinStudentScore")
          .setMaster("local")

        val sc = new SparkContext(conf)

        val DBName=Array(
          Tuple2(1,"Spark"),
          Tuple2(2,"Hadoop"),
          Tuple2(3,"Kylin"),
          Tuple2(4,"Flink"))

       val numType=Array(
         Tuple2(1,"String"),
         Tuple2(2,"int"),
         Tuple2(3,"byte"),
         Tuple2(4,"bollean"),
         Tuple2(5,"float"),
         Tuple2(1,"34"),
         Tuple2(1,"45"),
         Tuple2(2,"47"),
         Tuple2(3,"75"),
         Tuple2(4,"95"),
         Tuple2(5,"16"),
         Tuple2(1,"85"))

       val DBName1 = sc.parallelize(DBName)

       val numType1 = sc.parallelize(numType)

       val ex = DBName1.cogroup(numType1)

       ex.foreach( x=>
          println(x)
       )


  }
}