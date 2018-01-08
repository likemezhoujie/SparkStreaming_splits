package com.yida.sparkSql

import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

//todo:利用spark sql统计分析数据，最终将结果写入到mysql表中
case class People(id:Int,name:String,age:Int)
object DataToMysql {
  def main(args: Array[String]): Unit = {
    //创建SparkSession
    val spark: SparkSession = SparkSession.builder().appName("StructTypeSchema").master("local[2]").getOrCreate()
    //设置日志信息级别
    spark.sparkContext.setLogLevel("WARN")
    //获取sparkContext对象
    val sc: SparkContext = spark.sparkContext
    //读取数据文件
    val dataRDD: RDD[Array[String]] = sc.textFile("E:\\people.txt").map(x=>x.split(" "))
    //将RDD与样例类关联
    val peopleRDD: RDD[People] = dataRDD.map(x=>People(x(0).toInt,x(1).toString,x(2).toInt))
    //将RDD转换成dataFrame
    //这里需要手动导入一个隐式转换
    import spark.implicits._
    val peopleDF: DataFrame = peopleRDD.toDF()
    //把peopleDFS注册成一张表
    peopleDF.createTempView("t_people2")
    //按照年龄进行一个降序排列
    val sortDF: DataFrame = spark.sql("select * from t_people2 order by age DESC")
    //将sortDF写入到mysql中
    val url = "jdbc:mysql://localhost:3306/spark"
    val table = "t_people"
    val properties: Properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","root")
    //在向mysql表中插入数据的时候，可以指定插入模式mode
    //mode的配置选项：
    //   overwrite:覆盖
    //   append：追加
    //   ingore:只要有表存在，不做任何操作
    //   error:只要有表存在，报错
    sortDF.write.mode("append").jdbc(url,table,properties)
    sc.stop()

    spark.stop()
  }
}
