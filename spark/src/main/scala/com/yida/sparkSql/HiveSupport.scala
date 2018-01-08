package com.yida.sparkSql

import org.apache.spark.sql.SparkSession

//todo:利用spark sql操作hive sql
object HiveSupport {
  def main(args: Array[String]): Unit = {
    //创建SparkSession
    val spark: SparkSession = SparkSession.builder().appName("StructTypeSchema").master("local[2]").getOrCreate()
    //设置日志信息级别
    spark.sparkContext.setLogLevel("WARN")
    //利用hivesql创建一个表
    spark.sql("create table t1(id int,name string,age int) row format delimited fields terminated by ' '")
    //加载数据
    spark.sql("load data local inpath './data/people.txt' into table t1")
    //查询
    spark.sql("select * from t1").show()
    spark.stop()
  }
}
