package cmo.jd.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * Created by wangwei on 17-12-7.
  */
object Demo01 {

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      println("the args is " + args.length)
    }
    println(args(0).toString + "\n" + args(1).trim.toString)

    //    val spark = SparkSession.builder.appName("test").getOrCreate()
    val conf = new SparkConf().setAppName("testdemo").setMaster("local")
    val sc = new SparkContext(conf)
    val data = sc.textFile("/home/wangwei/software/spark-2.1.0-bin-hadoop2.7/README.md")
    val world = data.flatMap { line => line.split(" ") }
    val pairs = world.map { word => (word, 1) }
    val result = pairs.reduceByKey(_ + _).map(tuple => (tuple._2, tuple._1))
    val sorted = result.sortByKey(false).map(tuple => (tuple._2, tuple._1))
    sorted.foreach(x => println(x))

  }
}


class myTest {
  def Demo01(args: String) {
    val conf = new SparkConf().setMaster("local").setAppName("Demo01")
    conf.set("", "")
    val sc = new SparkContext(conf)
    val text = sc.textFile(args.toString).map(line => line.split("\t")).map(s => (s(0), s(2), s(3))).count()

  }
}
