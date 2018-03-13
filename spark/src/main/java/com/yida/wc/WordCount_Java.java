package com.yida.wc;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

//todo:利用java语言来实现spark的单词计数
public class WordCount_Java {
    public static void main(String[] args) {
        //todo：1、创建SparkConf对象，设置appName和master地址
        SparkConf sparkConf = new SparkConf().setAppName("WordCount_Java").setMaster("local[2]");

        //todo:2、创建javaSparkContext对象
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        //todo:3、读取数据文件
        JavaRDD<String> dataJavaRDD = jsc.textFile("E:\\aa.txt");

        //todo:4、对每一行进行切分压平
        JavaRDD<String> wordsJavaRDD = dataJavaRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override               //line表示每一行记录
            public Iterator<String> call(String line) throws Exception {
                //切分每一行
                String[] words = line.split(" "); //lll

                return Arrays.asList(words).iterator();
            }
        });

        //todo:5、每个单词记为1
        JavaPairRDD<String, Integer> wordAndOneJavaPairRDD = wordsJavaRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });

        //todo:6、把相同单词出现的次数累加  (_+_)
        JavaPairRDD<String, Integer> resultJavaPairRDD = wordAndOneJavaPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        //按照单词出现的次数降序排序
        //需要将（单词，次数）进行位置颠倒 （次数，单词）
        JavaPairRDD<Integer, String> sortJavaPairRDD = resultJavaPairRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
                return new Tuple2<>(t._2, t._1);
            }
        }).sortByKey(false);

        //将（次数，单词）变为（单词，次数）
        JavaPairRDD<String, Integer> finalSortJavaPairRDD = sortJavaPairRDD.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> t) throws Exception {
                return new Tuple2<>(t._2, t._1);
            }
        });

        //todo:7、收集打印
        List<Tuple2<String, Integer>> finalResult = finalSortJavaPairRDD.collect();

        for(Tuple2<String, Integer> t:finalResult){
            System.out.println(t);
        }

        jsc.stop();
    }
}
