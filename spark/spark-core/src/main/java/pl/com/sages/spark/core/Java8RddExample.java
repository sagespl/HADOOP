package pl.com.sages.spark.core;

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

public class Java8RddExample {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName(Java8RddExample.class.getName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("hdfs://...", 1);

        JavaRDD<String> words = lines.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")).iterator());

        JavaPairRDD<String, Integer> pairs = words.mapToPair(
                (PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1)
        );

        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((Function2<Integer, Integer, Integer>) (a, b) -> a + b);

        counts.saveAsTextFile("hdfs://...");
    }

}
