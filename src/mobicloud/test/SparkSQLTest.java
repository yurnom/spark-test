package mobicloud.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: He Qi
 * Date: 14-8-12
 * Time: 11:08
 */
public class SparkSQLTest {

public static class Person implements Serializable {
	private String name;
	private int age;

	public String getName() {
		return name;
	}

	public int getAge() {
		return age;
	}

	public Person(String name, int age) {
		this.name = name;
		this.age = age;
	}
}

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf()
			  .setAppName("JavaSparkSQL")
			  .setMaster("local[2]");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaSQLContext sqlCtx = new JavaSQLContext(ctx);

//		JavaRDD<Person> people = ctx.textFile("/home/nodin/people.txt")
//			  .map(line -> {
//			String[] parts = line.split(",");
//			return new Person(parts[0], Integer.parseInt(parts[1].trim()));
//		});
		JavaSchemaRDD schemaPeople = sqlCtx.jsonFile("/home/nodin/people.json");

//		JavaSchemaRDD schemaPeople = sqlCtx.applySchema(people, Person.class);
		schemaPeople.registerAsTable("people");
		JavaSchemaRDD teenagers = sqlCtx.sql(
			  "SELECT name FROM people WHERE age >= 13 AND age <= 19");

		List<String> teenagerNames = teenagers
			  .map(row -> "Name: " + row.getString(0)).collect();

		for(String s : teenagerNames) {
			System.out.println(s);
		}

		schemaPeople.saveAsParquetFile("people.parquet");
		JavaSchemaRDD parquetFile = sqlCtx.parquetFile("people.parquet");
		parquetFile.registerAsTable("parquetFile");
		JavaSchemaRDD teenagers2 = sqlCtx.sql("SELECT * FROM parquetFile WHERE age >= 25");
		for(Row r : teenagers2.collect()) {
			System.out.println(r.get(0));
			System.out.println(r.get(1));
		}
	}

	public static <T> void print(Collection<T> c) {
		for(T t : c) {
			System.out.println(t.toString());
		}
	}
}

