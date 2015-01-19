package mobicloud.tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;

/**
 * Created with IntelliJ IDEA.
 * User: He Qi
 * Date: 14-8-8
 * Time: 17:47
 */
public class LessThan implements Serializable {

	public static <T> void print(Collection<T> c) {
		for(T t : c) {
			System.out.println(t.toString());
		}
	}

	public static String convertScanToString(Scan scan) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(out);
		scan.write(dos);
		return Base64.encodeBytes(out.toByteArray());
	}

	public static void setUp() throws IOException {
		SparkConf sparkConf = new SparkConf()
			  .setAppName("Less Than Application")
			  .setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		JavaRDD<String> left = processTableLeft(sc);
		JavaRDD<String> right = processTableRight(sc);

		print(left.collect());
		System.out.println("----------------------------------");
		print(right.collect());
		System.out.println("----------------------------------");
		print(left.intersection(right).collect());
	}

	public static JavaRDD<String> processTableLeft(JavaSparkContext sc) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("SERV_I"));
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes("201404")));
		scan.setFilter(filter);
		scan.setCacheBlocks(false);
		scan.setCaching(500);

		conf.set(TableInputFormat.INPUT_TABLE, "bz_vas");
		conf.set(TableInputFormat.SCAN, convertScanToString(scan));

		JavaPairRDD<ImmutableBytesWritable, Result> vasRDD = sc.newAPIHadoopRDD(conf,
			  TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

		JavaRDD<String> vasID = vasRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, String>() {
			@Override
			public String call(Tuple2<ImmutableBytesWritable, Result> t) throws Exception {
				byte[] o = t._2().getValue(Bytes.toBytes("cf"), Bytes.toBytes("SERV_I"));
				if (o != null) return Bytes.toString(o);
				else return "";
			}
		});
//		print(vasID.collect());

		return vasID;
	}

	public static JavaRDD<String> processTableRight(JavaSparkContext sc) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("SERV_ID"));
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes("201403")));
		scan.setFilter(filter);
		scan.setCacheBlocks(false);
		scan.setCaching(500);

		conf.set(TableInputFormat.INPUT_TABLE, "bz_user");
		conf.set(TableInputFormat.SCAN, convertScanToString(scan));

		JavaPairRDD<ImmutableBytesWritable, Result> vasRDD = sc.newAPIHadoopRDD(conf,
			  TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

		JavaRDD<String> vasID = vasRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, String>() {
			@Override
			public String call(Tuple2<ImmutableBytesWritable, Result> t) throws Exception {
				byte[] o = t._2().getValue(Bytes.toBytes("cf"), Bytes.toBytes("SERV_ID"));
				if (o != null) return Bytes.toString(o);
				else return "";
			}
		});
//		print(vasID.collect());

		return vasID;
	}

	public static void main(String[] args) throws IOException {
		setUp();
	}
}
