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

public class Java7RddExample {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName(Java7RddExample.class.getName());
        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaRDD<String> lines = sc.textFile("hdfs://...", 1);

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        JavaPairRDD<String, Integer> pairs = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                }
        );

        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {

            public Integer call(Integer a, Integer b) {
                return a + b;
            }
        });

        counts.saveAsTextFile("hdfs://...");
    }

}
