package mobicloud.myMllib;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.configuration.Algo;
import org.apache.spark.mllib.tree.impurity.Gini;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;

import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 * User: He Qi
 * Date: 14-8-13
 * Time: 11:26
 */
public class JavaDTClassification {
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf()
			  .setAppName("LinearRegression")
			  .setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> data = sc.textFile("/home/nodin/data/sample_tree_data.csv");
		JavaRDD<LabeledPoint> parsedData = data.map(new Function<String, LabeledPoint>() {
			@Override
			public LabeledPoint call(String line) throws Exception {
				double[] doubles = Arrays.stream(line.split(","))
					  .mapToDouble(s -> Double.parseDouble(s))
					  .toArray();
				double[] values = new double[doubles.length-1];
				for(int i = 1; i < doubles.length; i++) {
					values[i - 1] = doubles[i];
				}
				return new LabeledPoint(doubles[0], Vectors.dense(values));
			}
		});

		int maxDepth = 5;
//		DecisionTreeModel model = DecisionTree.train(parsedData, Algo.Classification(), Gini.class, maxDepth);
	}
}
