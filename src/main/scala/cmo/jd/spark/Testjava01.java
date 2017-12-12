package cmo.jd.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by wangwei on 17-12-9.
 */
public class Testjava01 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("wc");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> text = sc.textFile("/home/wangwei/software/spark-2.1.0-bin-hadoop2.7/README.md");

        JavaRDD<String> words = text.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        JavaPairRDD<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));

        JavaPairRDD result = pairs.reduceByKey((a, b) -> a + b);
//
//        result.foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            @Override
//            public void call(Tuple2<String, Integer> tuple) throws Exception {
//                System.out.println(tuple._1 + "=>>" + tuple._2);
//            }
//        });
//
        result.foreach((Tuple2) -> System.out.println(Tuple2));

        sc.close();

    }


}



