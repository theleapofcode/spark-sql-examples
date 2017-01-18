package com.theleapofcode.spark.sql.example;

import static org.apache.spark.sql.functions.col;

import java.io.Serializable;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataFrameExamples {

	public static class User implements Serializable {

		private static final long serialVersionUID = 1L;

		private String fn;
		private String ln;
		private String alias;
		private int age;

		public String getFn() {
			return fn;
		}

		public void setFn(String fn) {
			this.fn = fn;
		}

		public String getLn() {
			return ln;
		}

		public void setLn(String ln) {
			this.ln = ln;
		}

		public String getAlias() {
			return alias;
		}

		public void setAlias(String alias) {
			this.alias = alias;
		}

		public int getAge() {
			return age;
		}

		public void setAge(int age) {
			this.age = age;
		}

	}

	public static void main(String[] args) throws AnalysisException {
		String fileName = "users.json";
		String path = null;
		if (args != null && args.length > 0) {
			path = args[0];
		} else {
			path = ClassLoader.getSystemResource(fileName).getPath();
		}
		SparkSession sparkSession = SparkSession.builder().master("local").appName("DataFrameExamples").getOrCreate();
		JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

		Dataset<Row> df = sparkSession.read().json(path);

		// Displays the content of the DataFrame to stdout
		df.show();

		// Print the schema in a tree format
		df.printSchema();

		// Select only the "alias" column
		df.select("alias").show();

		// Select everybody, but increment the age by 5
		df.select(col("alias"), col("age").plus(5)).show();

		// Select people older than 21
		df.filter(col("alias").rlike("[H].*")).show();

		// Count users by age
		df.groupBy("age").count().show();

		// Register the DataFrame as a SQL temporary view
		df.createOrReplaceTempView("users");

		Dataset<Row> sqlDF = sparkSession.sql("SELECT * FROM users");
		sqlDF.show();

		// Register the DataFrame as a global temporary view
		df.createGlobalTempView("users");

		// Global temporary view is tied to a system preserved database
		// `global_temp`
		sparkSession.sql("SELECT * FROM global_temp.users").show();

		// Global temporary view is cross-session
		sparkSession.newSession().sql("SELECT * FROM global_temp.users").show();

		// DataFrames can be converted to a Dataset by providing a class.
		Encoder<User> userEncoder = Encoders.bean(User.class);
		Dataset<User> usersDf = sparkSession.read().json(path).as(userEncoder);
		usersDf.show();

		sc.close();
	}

}
