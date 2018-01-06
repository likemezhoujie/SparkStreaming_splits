package com.yida.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//todo:通过scala编写park的单词技术程序
object WordCount {
  def main(args: Array[String]): Unit = {
    //todo:1.创建sparkConf对象，设置APPName和master地址，local[2]表示本地 使用2个线程来进行
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    //todo:2.创建sparkContext对象，
    val sc: SparkContext = new SparkContext(sparkConf)
    //设置日志输出级别
    sc.setLogLevel("WARN")
    //todo:3.读取数据文件
    val data: RDD[String] = sc.textFile("E:\\aa.txt")
    //todo：4.切分每一行并且压平
    val words: RDD[String] = data.flatMap(_.split(" "))
    //todo:5.每个单词记为1
    val wordAndOne: RDD[(String, Int)] = words.map((_,1))
    //todo:6.相同单词出现的次数加一
    val result: RDD[(String, Int)] = wordAndOne.reduceByKey(_+_)
    //todo:7.按照单词出现的次数降序排序
    val sortResult: RDD[(String, Int)] = result.sortBy(_._2,false)
    //todo:8.收集数据，打印输出
    val finalResult: Array[(String, Int)] = sortResult.collect()
    //todo:9.打印结果
    finalResult.foreach(x=>println(x))
    //println(finalResult.toBuffer)
    sc.stop()
  }
}
