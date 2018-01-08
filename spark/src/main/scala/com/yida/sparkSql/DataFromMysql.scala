package com.yida.sparkSql

import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

//todo:通过sparksql 读取mysql的数据
object DataFromMysql {
  def main(args: Array[String]): Unit = {
    //创建SparkSession
    val spark: SparkSession = SparkSession.builder().appName("StructTypeSchema").master("local[2]").getOrCreate()
    //设置日志信息级别
    spark.sparkContext.setLogLevel("WARN")
    val url = "jdbc:mysql://localhost:3306/spark"
    val table = "t_people"
    val properties: Properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","root")
    //通过sparkSession获取mysql的表信息
    val mysqlData: DataFrame = spark.read.jdbc(url,table,properties)
    //打印mysqlDataFrame的schema
    mysqlData.printSchema()
    //打印mysqlDataFrame的结果
    mysqlData.show()
    spark.stop()
  }
}
