package mobicloud.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * Created with IntelliJ IDEA.
 * User: He Qi
 * Date: 14-8-8
 * Time: 10:06
 */
public class SimpleApp {
	public static void main(String[] args) {
		String logFile = "/home/nodin/README.md"; // Should be some file on your system
//		String logFile = "/home/hadoop/spark-1.0.0-bin-hadoop1/README.md"; // Should be some file on your system
		SparkConf conf = new SparkConf()
			  .setAppName("Simple Application")
			  .setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> logData = sc.textFile(logFile).cache();

		long numAs = logData.filter(new Function<String, Boolean>() {
			public Boolean call(String s) { return s.contains("a"); }
		}).count();

		long numBs = logData.filter(new Function<String, Boolean>() {
			public Boolean call(String s) { return s.contains("b"); }
		}).count();

		System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
	}
}
