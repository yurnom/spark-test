package mobicloud.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;

/**
 * Created with IntelliJ IDEA.
 * User: He Qi
 * Date: 14-8-8
 * Time: 14:30
 */
public class WordCount {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
			  .setAppName("Simple Application")
			  .setMaster("local");
		JavaSparkContext spark = new JavaSparkContext(conf);
		JavaRDD<String> file = spark.textFile("/home/nodin/README.md");

		JavaRDD<String> words = file.flatMap(s -> Arrays.asList(s.split(" ")));
//		print(words.collect());
		JavaPairRDD<String, Integer> pairs = words.mapToPair(s -> new Tuple2(s, 1));
//		print(pairs.collect());
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);
//		print(counts.collect());

		counts.saveAsTextFile("/home/nodin/sparkR");
	}

	public static <T> void print(Collection<T> c) {
		for(T t : c) {
			System.out.println(t.toString());
		}
	}
}
