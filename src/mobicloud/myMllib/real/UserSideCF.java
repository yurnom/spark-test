package mobicloud.myMllib.real;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.Serializable;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: He Qi
 * Date: 14-9-4
 * Time: 14:17
 */
public class UserSideCF implements Serializable {

	private static final Pattern TAB = Pattern.compile("\t");

	public MatrixFactorizationModel buildModel(RDD<Rating> rdd) {
		int rank = 10;
		int numIterations = 20;
		MatrixFactorizationModel model = ALS.train(rdd, rank, numIterations, 0.01);
		return model;
	}

	public RDD<Rating>[] splitData() {
		SparkConf sparkConf = new SparkConf().setAppName("JavaALS").setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = sc.textFile("/home/nodin/ml-100k/u.data");
		JavaRDD<Rating> ratings = lines.map(line -> {
			String[] tok = TAB.split(line);
			int x = Integer.parseInt(tok[0]);
			int y = Integer.parseInt(tok[1]);
			double rating = Double.parseDouble(tok[2]);
			return new Rating(x, y, rating);
		});

		RDD<Rating>[] splits = ratings.rdd().randomSplit(new double[]{0.7,0.3}, 11L);
		//0.6,0.4 - 0.3706799281981904, 2.4569381099423957
		//0.8,0.2 - 0.4335027003381571, 2.0930908173274476
		//0.9,0.1 - 0.4587619714761296, 1.7213014771993198
		return splits;
	}

	public static void main(String[] args) {
		UserSideCF cf = new UserSideCF();
		RDD<Rating>[] splits = cf.splitData();
		MatrixFactorizationModel model = cf.buildModel(splits[0]);


		Double MSE = cf.getMSE(splits[0].toJavaRDD(), model);
		System.out.println("Mean Squared Error = " + MSE);

		Double MSE1 = cf.getMSE(splits[1].toJavaRDD(), model);
		System.out.println("Mean Squared Error1 = " + MSE1);

	}

	public Double getMSE(JavaRDD<Rating> ratings, MatrixFactorizationModel model) {
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

		return joins.mapToDouble(new DoubleFunction<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>>() {
			@Override
			public double call(Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> o) throws Exception {
				double err = o._2()._1() - o._2()._2();
				return err * err;
			}
		}).mean();
	}
}
