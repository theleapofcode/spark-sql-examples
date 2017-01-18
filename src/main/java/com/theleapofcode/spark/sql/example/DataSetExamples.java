package com.theleapofcode.spark.sql.example;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DataSetExamples {
	public static class Person implements Serializable {

		private static final long serialVersionUID = 1L;

		private String name;
		private int age;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public int getAge() {
			return age;
		}

		public void setAge(int age) {
			this.age = age;
		}

		public Person() {
			super();
		}

		public Person(String name, int age) {
			super();
			this.name = name;
			this.age = age;
		}

	}

	public static void main(String[] args) throws AnalysisException {
		String fileName = "persons.txt";
		String path = null;
		if (args != null && args.length > 0) {
			path = args[0];
		} else {
			path = ClassLoader.getSystemResource(fileName).getPath();
		}

		SparkSession sparkSession = SparkSession.builder().master("local").appName("DataSetExamples").getOrCreate();
		JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

		// Create an instance of a Bean class
		Person person = new Person();
		person.setName("Clark");
		person.setAge(30);

		// Encoders are created for Java beans
		Encoder<Person> personEncoder = Encoders.bean(Person.class);
		Dataset<Person> javaBeanDS = sparkSession.createDataset(Collections.singletonList(person), personEncoder);
		javaBeanDS.show();

		// Encoders for most common types are provided in class Encoders
		Encoder<Integer> integerEncoder = Encoders.INT();
		Dataset<Integer> primitiveDS = sparkSession.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
		Dataset<Integer> transformedDS = primitiveDS.map(x -> x + 1, integerEncoder);
		Integer[] result = (Integer[]) transformedDS.collect();
		Arrays.asList(result).forEach(System.out::println);

		// Creation of DataSet using Reflection

		// Create an RDD of Person objects from a text file
		JavaRDD<Person> personRDD = sparkSession.read().textFile(path).javaRDD()
				.map(x -> new Person(x.split(",")[0], Integer.parseInt(x.split(",")[1])));

		// Apply a schema to an RDD of JavaBeans to get a DataFrame
		Dataset<Row> personDF = sparkSession.createDataFrame(personRDD, Person.class);
		// Register the DataFrame as a temporary view
		personDF.createOrReplaceTempView("person");

		// SQL statements can be run by using the sql methods provided by spark
		Dataset<Row> teenageSuperHeroesDF = sparkSession.sql("SELECT * FROM person WHERE age BETWEEN 13 AND 19");
		teenageSuperHeroesDF.show();

		// The columns of a row in the result can be accessed by field index
		Encoder<String> stringEncoder = Encoders.STRING();
		Dataset<String> teenageSuperHeroNamesByIndexDF = teenageSuperHeroesDF.map(row -> row.getString(1),
				stringEncoder);
		teenageSuperHeroNamesByIndexDF.show();

		// The columns of a row in the result can be accessed by field name
		Dataset<String> teenageNamesByNameDF = teenageSuperHeroesDF.map(row -> row.getAs("name"), stringEncoder);
		teenageNamesByNameDF.show();

		// Creation of DataSet using Schema

		JavaRDD<String> personStringRDD = sparkSession.sparkContext().textFile(path, 1).toJavaRDD();

		// The schema is encoded in a string
		String schemaString = "name age";

		// Generate the schema based on the string of schema
		List<StructField> fields = new ArrayList<>();
		for (String fieldName : schemaString.split(" ")) {
			StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
			fields.add(field);
		}
		StructType schema = DataTypes.createStructType(fields);

		// Convert records of the RDD (person) to Rows
		JavaRDD<Row> peopleRowRDD = personStringRDD
				.map(line -> RowFactory.create(line.split(",")[0], line.split(",")[1]));

		// Apply the schema to the RDD
		Dataset<Row> personDataFrame = sparkSession.createDataFrame(peopleRowRDD, schema);

		// Creates a temporary view using the DataFrame
		personDataFrame.createOrReplaceTempView("Persons");

		// SQL can be run over a temporary view created using DataFrames
		Dataset<Row> results = sparkSession.sql("SELECT * FROM Persons");
		results.show();

		sc.close();
	}

}
