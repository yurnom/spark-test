package mobicloud.streaming;

import com.google.common.base.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.examples.streaming.StreamingExamples;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: He Qi
 * Date: 14-8-11
 * Time: 11:24
 */
public class WordCount {
	public static <T> void print(Collection<T> c) {
		for(T t : c) {
			System.out.println(t.toString());
		}
	}

	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) {
		StreamingExamples.setStreamingLogLevels();

		JavaStreamingContext jssc = new JavaStreamingContext("local[2]",
			  "JavaNetworkWordCount", new Duration(1000));
		JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
		JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(SPACE.split(line)));
		JavaPairDStream<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));
		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> (i1 + i2));

		wordCounts.print();

		jssc.start();              // Start the computation
		jssc.awaitTermination();   // Wait for the computation to terminate
	}
}
