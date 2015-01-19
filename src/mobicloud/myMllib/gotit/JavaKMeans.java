package mobicloud.myMllib.gotit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.util.Arrays;
import java.util.Collection;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: He Qi
 * Date: 14-8-15
 * Time: 14:34
 */
public class JavaKMeans {

	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf()
			  .setAppName("K-Means")
			  .setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> data = sc.textFile("/home/nodin/data/kmeans_data.txt");
		JavaRDD<Vector> parsedData = data.map(s -> {
			double[] values = Arrays.asList(SPACE.split(s))
				  .stream()
				  .mapToDouble(Double::parseDouble)
				  .toArray();
			return Vectors.dense(values);
		});

		int numClusters = 3;
		int numIterations = 20;
		int runs = 10;
		KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations, runs);

		print(parsedData.map(v -> v.toString()
			  + " belong to cluster :" + clusters.predict(v)).collect());

		double wssse = clusters.computeCost(parsedData.rdd());
		System.out.println("Within Set Sum of Squared Errors = " + wssse);

		System.out.println("Cluster centers:");
		for (Vector center : clusters.clusterCenters()) {
			System.out.println(" " + center);
		}

		System.out.println("Prediction of (1.1, 2.1, 3.1): "
			  + clusters.predict(Vectors.dense(new double[]{1.1, 2.1, 3.1})));
		System.out.println("Prediction of (10.1, 9.1, 11.1): "
			  + clusters.predict(Vectors.dense(new double[]{10.1, 9.1, 11.1})));
		System.out.println("Prediction of (21.1, 17.1, 16.1): "
			  + clusters.predict(Vectors.dense(new double[]{21.1, 17.1, 16.1})));
	}

	public static <T> void print(Collection<T> c) {
		for(T t : c) {
			System.out.println(t.toString());
		}
	}
}
