package com.mireyi.bukex.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01WordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("wordcount")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("datas")

    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)

    val result: RDD[(String, Int)] = wordGroup.mapValues(value => value.size)

    result.foreach(println)

    sc.stop()

  }
}
