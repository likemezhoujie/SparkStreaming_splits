package com.yida.net

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

//todo:利用spark统计运营商uv总量
object UV extends App{
  //创建sparkConf对象
  private val sparkConf: SparkConf = new SparkConf().setAppName("PV").setMaster("local[2]")
  //创建SparkContext对象
  private val sc: SparkContext = new SparkContext(sparkConf)
  //设置输出的日志级别
  sc.setLogLevel("WARN")
  //读取日志数据
  private val dataRDD: RDD[String] = sc.textFile("E:\\access.log")
  //切分每一行，获取对应的ip地址
  private val ips: RDD[String] = dataRDD.map(_.split(" ")(0))
  //去重
  private val ipNum: Long = ips.distinct().count()
  println(ipNum)
  //g关闭资源
  sc.stop()
}
