package mobicloud.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 * User: He Qi
 * Date: 14-8-8
 * Time: 15:49
 */
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.function.Function2;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;

public class HBaseTest implements Serializable {

	public Log log = LogFactory.getLog(HBaseTest.class);

	/**
	 * 将scan编码，该方法copy自 org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil
	 *
	 * @param scan
	 * @return
	 * @throws IOException
	 */
	static String convertScanToString(Scan scan) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(out);
		scan.write(dos);
		return Base64.encodeBytes(out.toByteArray());
	}

	public void start() {
		//初始化sparkContext，这里必须在jars参数里面放上Hbase的jar，
		// 否则会报unread block data异常
//		JavaSparkContext sc = new JavaSparkContext("spark://nowledgedata-n3:7077", "hbaseTest",
//			  "/home/hadoop/software/spark-0.8.1",
//			  new String[]{"target/ndspark.jar", "target\\dependency\\hbase-0.94.6.jar"});
		SparkConf sparkConf = new SparkConf()
			  .setAppName("Simple Application")
			  .setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		//使用HBaseConfiguration.create()生成Configuration
		// 必须在项目classpath下放上hadoop以及hbase的配置文件。
		Configuration conf = HBaseConfiguration.create();
		//设置查询条件，这里值返回用户的等级
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("cf1"));
		scan.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));

		try {
			//需要读取的hbase表名
			String tableName = "pigstore";
			conf.set(TableInputFormat.INPUT_TABLE, tableName);
			conf.set(TableInputFormat.SCAN, convertScanToString(scan));

			//获得hbase查询结果Result
			JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = sc.newAPIHadoopRDD(conf,
				  TableInputFormat.class, ImmutableBytesWritable.class,
				  Result.class);

			System.out.println(hBaseRDD.count());

			JavaRDD<String> my01 = hBaseRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, String>() {
				@Override
				public String call(Tuple2<ImmutableBytesWritable, Result> t) throws Exception {
					byte[] o = t._2().getValue(
						  Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
					if (o != null) {
						return Bytes.toString(o);
					}
					return "";
				}
			});

			print(my01.collect());

		} catch (Exception e) {
			log.warn(e);
		}

	}

	/**
	 * spark如果计算没写在main里面,实现的类必须继承Serializable接口，<br>
	 * </>否则会报 Task not serializable: java.io.NotSerializableException 异常
	 */
	public static void main(String[] args) throws InterruptedException {
		new HBaseTest().start();
		System.exit(0);
	}

	public static <T> void print(Collection<T> c) {
		for(T t : c) {
			System.out.println(t.toString());
		}
	}
}
