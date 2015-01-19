package mobicloud.myMllib;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.optimization.L1Updater;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.rdd.RDD;
import scala.Function1;
import scala.Tuple2;

/**
 * Created with IntelliJ IDEA.
 * User: He Qi
 * Date: 14-8-12
 * Time: 18:14
 */
public class JavaSVM {
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("SVM").setMaster("local[2]");
		SparkContext sc = new SparkContext(sparkConf);
		RDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc, "/home/nodin/data/sample_libsvm_data.txt");

		RDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.6, 0.4}, 11L);
		RDD<LabeledPoint> training = splits[0].cache();
		RDD<LabeledPoint> test = splits[1];
		int numIterations = 100;

		SVMModel model = SVMWithSGD.train(training, numIterations);
		JavaPairRDD scoreAndLabels = test.toJavaRDD().mapToPair(point ->
			  new Tuple2<>(model.predict(point.features()), point.label()));
		BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(scoreAndLabels.rdd());

		LogisticRegressionModel model1 = LogisticRegressionWithSGD.train(training, numIterations);
		model1.clearThreshold();
		JavaPairRDD scoreAndLabels1 = test.toJavaRDD().mapToPair(point ->
			  new Tuple2<>(model1.predict(point.features()), point.label()));
		BinaryClassificationMetrics metrics1 = new BinaryClassificationMetrics(scoreAndLabels1.rdd());

		System.out.println("SVM Area under ROC = " + metrics.areaUnderROC());
		System.out.println("SVM Area under PR = " + metrics.areaUnderPR());
		System.out.println("Logistic Area under ROC = " + metrics1.areaUnderROC());
		System.out.println("Logistic Area under PR = " + metrics1.areaUnderPR());

//		SVMWithSGD svmAlg = new SVMWithSGD();
//		svmAlg.optimizer()
//			  .setNumIterations(200)
//			  .setRegParam(0.1)
//			  .setUpdater(new L1Updater());
//		SVMModel model1 = svmAlg.run(training);
	}
}
