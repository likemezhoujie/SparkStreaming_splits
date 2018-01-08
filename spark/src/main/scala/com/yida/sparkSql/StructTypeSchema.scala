package com.yida.sparkSql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

//todo:利用sparksql 将RDD转化为DataFrame-------通StrctType手动指定schema
object StructTypeSchema {
  def main(args: Array[String]): Unit = {
    //创建SparkSession
    val spark: SparkSession = SparkSession.builder().appName("StructTypeSchema").master("local[2]").getOrCreate()
    //获取sparkContext对象
    val sc: SparkContext = spark.sparkContext
    //读取数据文件
    val dataRDD = sc.textFile("E:\\people.txt").map(x=>x.split(" "))
    //创建rowRDD
    val rowRDD: RDD[Row] = dataRDD.map(x=>Row(x(0).toInt,x(1).toString,x(2).toInt))
    // 指定dataFrame中的schme 用到StructType对象
    val structType =StructType(
      StructField("id", IntegerType, false) ::
        StructField("name", StringType, false) ::
        StructField("age", IntegerType, true) :: Nil)
    //调用sparkSession.createDataFrame生成DataFrame
    val df: DataFrame = spark.createDataFrame(rowRDD,structType)
    // 打印dataFrame的chema
    df.printSchema()
    //打印dataFrame的数据
    df.show()
    // df注册成一张表
    df.createTempView("t_people1")
    spark.sql("select * from t_people1 order by age desc").show()
    sc.stop()
    spark.stop()
  }
}
