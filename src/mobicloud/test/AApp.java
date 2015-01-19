package mobicloud.test;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: He Qi
 * Date: 14-8-8
 * Time: 10:33
 */
public class AApp {

	public static <T> void print(Collection<T> c) {
		for(T t : c) {
			System.out.println(t.toString());
		}
	}

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
			  .setAppName("A Application")
			  .setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
		List<Integer> data1 = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> distData = sc.parallelize(data);
		JavaRDD<Integer> distData1 = sc.parallelize(data1);

//		System.out.println(distData.reduce((a, b) -> a + b)); //15

//		print(distData.map(a -> a + "dd").collect());

//		print(distData.filter(a -> a < 3 ? true: false).collect());

//		print(distData.flatMap(new FlatMapFunction<Integer, String>() {
//			@Override
//			public Iterable<String> call(Integer integer) throws Exception {
//				List<String> list = Lists.newArrayList();
//				for(int i = 0; i < integer; i++) {
//					list.add("tag" + integer + ":" + i);
//				}
//				return list;
//			}
//		}).collect());

//		System.out.println(distData.mapPartitions(new FlatMapFunction<Iterator<Integer>, String>() {
//			@Override
//			public Iterable<String> call(Iterator<Integer> integerIterator) throws Exception {
//				List<String> list = Lists.newArrayList();
//				while (integerIterator.hasNext()) {
//					int t = integerIterator.next();
//					System.out.println(t);
//					list.add(t+"ff");
//				}
//				return list;
//			}
//		}).count());

//		System.out.println(distData.sample(true, 0.2).count());

		JavaRDD<Integer> distDataN = distData.union(distData1);
//		print(distDataN.collect());
		print(distDataN.distinct().collect());

		print(distDataN.groupBy(new Function<Integer, String>() {
			@Override
			public String call(Integer integer) throws Exception {
				System.out.println(integer);
				return integer.toString()+":";
			}
		}).collect());
	}
}
