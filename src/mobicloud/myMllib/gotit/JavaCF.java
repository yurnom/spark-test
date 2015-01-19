package mobicloud.myMllib.gotit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import scala.Tuple2;

import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: He Qi
 * Date: 14-8-13
 * Time: 15:12
 * 二元分类
 * data
 1,1,5.0
 1,2,1.0
 1,3,5.0
 1,4,1.0
 2,1,5.0
 2,2,1.0
 2,3,5.0
 2,4,1.0
 3,1,1.0
 3,2,5.0
 3,3,1.0
 3,4,5.0
 4,1,1.0
 4,2,5.0
 4,3,1.0
 4,4,5.0
 */
public class JavaCF {
	private static final Pattern COMMA = Pattern.compile(",");

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("JavaALS").setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = sc.textFile("/home/nodin/data/als/test.data");
		JavaRDD<Rating> ratings = lines.map(line -> {
			String[] tok = COMMA.split(line);
			int x = Integer.parseInt(tok[0]);
			int y = Integer.parseInt(tok[1]);
			double rating = Double.parseDouble(tok[2]);
			return new Rating(x, y, rating);
		});

		int rank = 10;
		int numIterations = 20;
		MatrixFactorizationModel model = ALS.train(ratings.rdd(), rank, numIterations, 0.01);

		System.out.println("Prediction of (2, 1):" + model.predict(2, 1));//Prediction of (2, 1):4.166456075109896
		System.out.println("Prediction of (2, 4):" + model.predict(2, 4));//Prediction of (2, 4):1.5327442301985683
		System.out.println("Prediction of (4, 2):" + model.predict(4, 2));//Prediction of (4, 2):4.0682986006368775
		System.out.println("Prediction of (4, 3):" + model.predict(4, 3));//Prediction of (4, 3):1.5917315963604075

		JavaPairRDD usersProducts = ratings.mapToPair(rating -> new Tuple2<>(rating.user(), rating.product()));
		JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = model.predict(usersProducts.rdd())
			  .toJavaRDD()
			  .mapToPair(new PairFunction<Rating, Tuple2<Integer, Integer>, Double>() {
				  @Override
				  public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating rating) throws Exception {
					  return new Tuple2<>(new Tuple2<>(rating.user(), rating.product()), rating.rating());
				  }
			  });

		JavaPairRDD<Tuple2<Integer, Integer>, Double> ratesAndPreds = ratings
			  .mapToPair(new PairFunction<Rating, Tuple2<Integer, Integer>, Double>() {
				  @Override
				  public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating rating) throws Exception {
					  return new Tuple2<>(new Tuple2<>(rating.user(), rating.product()), rating.rating());
				  }
			  });
		JavaPairRDD joins = ratesAndPreds.join(predictions);

		Double MSE = joins.mapToDouble(new DoubleFunction<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>>() {
			@Override
			public double call(Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> o) throws Exception {
				double err = o._2()._1() - o._2()._2();
				return err * err;
			}
		}).mean();
		System.out.println("Mean Squared Error = " + MSE);
	}
}
