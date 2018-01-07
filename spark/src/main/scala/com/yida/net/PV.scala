package com.yida.net

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//todo:利用Spark程序统计运营商pv总量
object PV extends App{
  //创建sparkConf对象
  private val sparkConf: SparkConf = new SparkConf().setAppName("PV").setMaster("local[2]")
  //创建SparkContext对象
  private val sc: SparkContext = new SparkContext(sparkConf)
  //设置输出的日志级别
  sc.setLogLevel("WARN")
  //读取日志数据
  private val dataRDD: RDD[String] = sc.textFile("E:\\access.log")
  //统计pv总量====方式一：计算有多少行及pv总量
  private val finalResult1: Long = dataRDD.count()
  println(finalResult1)
  //方式二：每一条日志信息记为一条数据1
  private val pvOne: RDD[(String, Int)] = dataRDD.map(x=>("PV",1))
  //对pv根据key进行累加
  private val resultPV: RDD[(String, Int)] = pvOne.reduceByKey(_+_)
  //打印pv总量
  resultPV.foreach(x=>println(x))
  //关闭资源
  sc.stop()
}
