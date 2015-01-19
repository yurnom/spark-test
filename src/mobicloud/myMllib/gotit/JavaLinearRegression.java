package mobicloud.myMllib.gotit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: He Qi
 * Date: 14-8-13
 * Time: 10:50
 * 线性回归
 */
public class JavaLinearRegression {
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf()
			  .setAppName("Regression")
			  .setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> data = sc.textFile("/home/nodin/data/ridge-data/lpsa.data");
		JavaRDD<LabeledPoint> parsedData = data.map(line -> {
			String[] parts = line.split(",");
			double[] ds = Arrays.stream(parts[1].split(" "))
				  .mapToDouble(Double::parseDouble)
				  .toArray();
			return new LabeledPoint(Double.parseDouble(parts[0]), Vectors.dense(ds));
		}).cache();

		int numIterations = 25;
		LinearRegressionModel model = LinearRegressionWithSGD.train(parsedData.rdd(), numIterations);
		RidgeRegressionModel model1 = RidgeRegressionWithSGD.train(parsedData.rdd(), numIterations);
		LassoModel model2 = LassoWithSGD.train(parsedData.rdd(), numIterations);

		print(parsedData, model);//training Mean Squared Error = 6.206807793307759
		print(parsedData, model1);//training Mean Squared Error = 6.416002077543526
		print(parsedData, model2);//training Mean Squared Error = 6.972349839013683

		double[] d = new double[]{1.0, 1.0, 2.0, 1.0, 3.0, -1.0, 1.0, -2.0};
		Vector v = Vectors.dense(d);
		System.out.println("Prediction of linear: " + model.predict(v));
		System.out.println("Prediction of ridge: " + model1.predict(v));
		System.out.println("Prediction of lasso: " + model2.predict(v));
	}

	public static void print(JavaRDD<LabeledPoint> parsedData, GeneralizedLinearModel model) {
		JavaPairRDD<Double, Double> valuesAndPreds = parsedData.mapToPair(point -> {
			double prediction = model.predict(point.features());
			return new Tuple2<>(point.label(), prediction);
		});

		Double MSE = valuesAndPreds.mapToDouble((Tuple2<Double, Double> t) -> Math.pow(t._1() - t._2(), 2)).mean();
		System.out.println(model.getClass().getName() + " training Mean Squared Error = " + MSE);
	}
}
