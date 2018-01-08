package com.yida.sparkSql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

//todo:利用sparkSql来创建DataFrame----反射机制(case class)

case class People1(id:Int,name:String,age:Int)
object ReflectSchema {
  def main(args: Array[String]): Unit = {
    //构建sparkSession
    val spark: SparkSession = SparkSession.builder().appName("ReflectSchema").master("local[2]").getOrCreate()
    //获取sparkContext对象
    val sc: SparkContext = spark.sparkContext
    //spark
    //读取数据文件
    val dataRDD: RDD[Array[String]] = sc.textFile("E:\\people.txt").map(x=>x.split(" "))
    //将RDD与样例类关联
    val peopleRDD: RDD[People1] = dataRDD.map(x=>People1(x(0).toInt,x(1).toString,x(2).toInt))
    //创建dataFrame，需要导入隐式转换
    import spark.implicits._
    val personDF: DataFrame = peopleRDD.toDF()
    //todo-------------------DSL语法操作 start--------------
    //1、显示DataFrame的数据，默认显示20行
    personDF.show()
    //2、显示DataFrame的schema信息
    personDF.printSchema()
    //3、显示DataFrame记录数
    println(personDF.count())
    //4、显示DataFrame的所有字段
    personDF.columns.foreach(println)
    //5、取出DataFrame的第一行记录
    println(personDF.first())
    println(personDF.head(2).toBuffer)
    //6、显示DataFrame中name字段的所有值
    personDF.select("name").show()
    //7、过滤出DataFrame中年龄大于30的记录
    personDF.filter($"age">30).show()
    //8、统计DataFrame中按照年龄进行分组，求每个组的人数
    personDF.groupBy("age").count().show()
    //todo-------------------DSL语法操作 end-------------
    println("============================================================")
    //todo--------------------SQL操作风格 start-----------
    //todo:将DataFrame注册成表
    //personDF.registerTempTable("t_people")
    personDF.createTempView("t_people")
    //可以通过sparkSession.sql(sql语句)
    spark.sql("select * from t_people").show()
    //查询id=1记录
    spark.sql("select * from t_people where id=1").show()
    //todo-----------------------SQL语法 end------------------
    sc.stop()
    spark.stop()
  }
}
