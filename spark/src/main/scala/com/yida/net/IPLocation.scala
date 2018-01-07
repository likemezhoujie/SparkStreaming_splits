package com.yida.net

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

//todo:根据ip查询区域
object IPLocation extends App{
  //创建sparkConf对象
  private val sparkConf: SparkConf = new SparkConf().setAppName("PV").setMaster("local[2]")
  //创建SparkContext对象
  private val sc: SparkContext = new SparkContext(sparkConf)
  //设置输出的日志级别
  sc.setLogLevel("WARN")
  //读取ip基站区域数据
  private val jizhanRDD: RDD[String] = sc.textFile("E:\\ip.txt")
  //获取IPStart、IPEnd、经度、纬度
  private val jizhanTupleRDD: RDD[(String, String, String, String)] = jizhanRDD.map(_.split("\\|")).map(x=>(x(2),x(3),x(13),x(14)))
  //把基站数据设置成广播变量
  private val jizhanBroadcast: Broadcast[Array[(String, String, String, String)]] = sc.broadcast(jizhanTupleRDD.collect())
  //加载运营商日志数据
  private val dataRDD: RDD[String] = sc.textFile("E:\\20090121000132.394251.http.format")
  //获取日志数据中的ip
  private val ipsRDD: RDD[String] = dataRDD.map(_.split("\\|")(1))
  //匹配对应的ip地址

  //ip地址转换为Long
  def ip2Long(ip: String): Long = {
    val ipArray: Array[String] = ip.split("\\.")
    var ipNum:Long = 0L
    for(i<- ipArray){
      ipNum = i.toLong | ipNum << 8L
    }
    ipNum
  }

  //通过二分查找找到对应ip地址在基站数组数据中的下标
  def binarySearch(ipNum: Long, ipsData: Array[(String, String, String, String)]): Int = {
    var start:Int = 0
    var end:Int = ipsData.length-1
    //var
    while (start<=end){
      var middle = (start+end)/2
      if(ipNum>=ipsData(middle)._1.toLong && ipNum<=ipsData(middle)._2.toLong){
        return middle
      }
      if(ipNum<ipsData(middle)._1.toLong){
        end = middle
      }
      if(ipNum>ipsData(middle)._2.toLong){
        start = middle
      }
    }
    -1
  }

  private val result: RDD[((String, String), Int)] = ipsRDD.mapPartitions(iter => {
    //先获取广播变量的值
    val ipsData: Array[(String, String, String, String)] = jizhanBroadcast.value
    //遍历iter迭代器中的数据
    iter.map(ip => {
      //ip地址转Long
      val ipNum: Long = ip2Long(ip)
      //奖IPNum去IPSData去匹配，获取对应ip在ipsData中的下标
      val index: Int = binarySearch(ipNum, ipsData)
      //获取该ip在基站数据中的经度和纬度
      ((ipsData(index)._3, ipsData(index)._4), 1)
    })
  })
  private val finalResult: RDD[((String, String), Int)] = result.reduceByKey(_+_)
  //打印输出
  finalResult.collect().foreach(println)
  sc.stop()
}
