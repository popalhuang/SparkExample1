package org.apache.spark.examples.SparkSQL;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import com.cloudera.livy.shaded.apache.commons.codec.Encoder;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.floor;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.upper;


public class Example3 {
	public static class EmployeeDep implements Serializable{
		private String eid="";
		private String name="";
		private String gender="F";
		private int age=0;
		private List<String> relative = new ArrayList<String>();
		public EmployeeDep(String eid,String name,String gender,int age,List<String> relative){
			this.eid=eid;
			this.name=name;
			this.gender=gender;
			this.age=age;
			this.relative=relative;
		}
		public String getEid() {
			return eid;
		}
		public void setEid(String eid) {
			this.eid = eid;
		}
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public String getGender() {
			return gender;
		}
		public void setGender(String gender) {
			this.gender = gender;
		}
		public int getAge() {
			return age;
		}
		public void setAge(int age) {
			this.age = age;
		}
		public List<String> getRelative() {
			return relative;
		}
		public void setRelative(List<String> relative) {
			this.relative = relative;
		}
		
	}
	public static class Employee implements Serializable {
		public String eid="";		
		public String jobTitle="";
		public int salary=0;
		
		public Employee(){
			
		}
		public Employee(String eid,String jobTitle,int salary){
			this.eid=eid;
			this.jobTitle=jobTitle;
			this.salary=salary;
		}
			
		public String getJobTitle() {
			return jobTitle;
		}

		public void setJobTitle(String jobTitle) {
			this.jobTitle = jobTitle;
		}

		public int getSalary() {
			return salary;
		}
		public void setSalary(int salary) {
			this.salary = salary;
		}
		public String getEid() {
			return eid;
		}
		public void setEid(String eid) {
			this.eid = eid;
		}
		
		
	}
	public static void  main(String[] args) throws AnalysisException{
		
		SparkSession spark = SparkSession.builder().appName("SparkSQL.Example3").enableHiveSupport().getOrCreate();
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		spark.sparkContext().setLogLevel("ERROR");
		
		List<Employee> employees = Arrays.asList(
				new Employee("001","engineer",10000),
				new Employee("002","engineer",30000),
				new Employee("003","director",50000),				
				new Employee("004","manager ",70000),
				new Employee("005","chairman",90000),
				new Employee("007","chairman",100000)
		);
		List<EmployeeDep> employeesdep = Arrays.asList(
				new EmployeeDep("001","Tom","F",20,Arrays.asList("aaa","bbb")),
				new EmployeeDep("003","Kevin","F",38,Arrays.asList("ccc","ddd")),
				new EmployeeDep("004","Ted","F",30,Arrays.asList("eee","fff")),
				new EmployeeDep("005","Amy","M",40,Arrays.asList("ggg","hhh")),
				new EmployeeDep("006","Iran","M",40,Arrays.asList("iii","jjj")),
				new EmployeeDep("007","Eric","F",40,Arrays.asList("kkk","lll"))
		);
		
		JavaRDD<Employee> jrdd = jsc.parallelize(employees);
		RDD<Employee> rdd1 = jrdd.rdd();
		RDD<Employee> rdd2 = jrdd.toRDD(jrdd);
		
		
		
		
		Dataset<Row> ds_employee = spark.createDataFrame(employees, Employee.class);		
		ds_employee.show();
		/*  result:
		  	+---+--------+------+
			|eid|jobTitle|salary|
			+---+--------+------+
			|001|engineer| 10000|
			|002|engineer| 30000|
			|003|director| 50000|
			|004|manager | 70000|
			|005|chairman| 90000|
			|007|chairman|100000|
			+---+--------+------+
		*/
		
		Dataset<Row> ds_employeedep = spark.createDataFrame(employeesdep, EmployeeDep.class);		
		ds_employeedep.show();
		/*  result:
		 	+---+---+------+-----+----------+
			|age|eid|gender| name|  relative|
			+---+---+------+-----+----------+
			| 20|001|     F|  Tom|[aaa, bbb]|
			| 38|003|     F|Kevin|[ccc, ddd]|
			| 30|004|     F|  Ted|[eee, fff]|
			| 40|005|     M|  Amy|[ggg, hhh]|
			| 40|006|     M| Iran|[iii, jjj]|
			| 40|007|     F| Eric|[kkk, lll]|
			+---+---+------+-----+----------+
		 */
		
		//Create DataFrame Example
		//1.List<T> --> DataFrame
		Dataset<Row> df1 = spark.createDataFrame(employees, Employee.class);		
		df1.show();
		/*  result:
		  	+---+--------+------+
			|eid|jobTitle|salary|
			+---+--------+------+
			|001|engineer| 10000|
			|002|engineer| 30000|
			|003|director| 50000|
			|004|manager | 70000|
			|005|chairman| 90000|
			|007|chairman|100000|
			+---+--------+------+

		 */
		
		//2.JavaRDD --> DataFrame
		Dataset<Row> df2 = spark.createDataFrame(jrdd,Employee.class);
		df2.show();
		/*  result:
		 	+---+--------+------+
			|eid|jobTitle|salary|
			+---+--------+------+
			|001|engineer| 10000|
			|002|engineer| 30000|
			|003|director| 50000|
			|004|manager | 70000|
			|005|chairman| 90000|
			|007|chairman|100000|
			+---+--------+------+ 
		 */
		
		//3.RDD --> DataFrame
		Dataset<Row> df3 = spark.createDataFrame(rdd1,Employee.class);
		df3.show();
		/*  result:
		 	+---+--------+------+
			|eid|jobTitle|salary|
			+---+--------+------+
			|001|engineer| 10000|
			|002|engineer| 30000|
			|003|director| 50000|
			|004|manager | 70000|
			|005|chairman| 90000|
			|007|chairman|100000|
			+---+--------+------+ 
		 */
	
		//Create Dataset Example
		//1. List<T> --> Dataset
		Dataset<Employee> ds1 = spark.createDataset(employees,Encoders.bean(Employee.class));
		ds1.show();
		/*  result:
		 	+---+--------+------+
			|eid|jobTitle|salary|
			+---+--------+------+
			|001|engineer| 10000|
			|002|engineer| 30000|
			|003|director| 50000|
			|004|manager | 70000|
			|005|chairman| 90000|
			|007|chairman|100000|
			+---+--------+------+ 
		*/
		
		//2. RDD<T> --> Dataset		
		Dataset<Employee> ds2 = spark.createDataset(rdd1, Encoders.bean(Employee.class));
		ds2.show();
		/*  result:
		 	+---+--------+------+
			|eid|jobTitle|salary|
			+---+--------+------+
			|001|engineer| 10000|
			|002|engineer| 30000|
			|003|director| 50000|
			|004|manager | 70000|
			|005|chairman| 90000|
			|007|chairman|100000|
			+---+--------+------+ 
		 */
		
		//Create empty Dataset
		Dataset<Employee> ds3 = spark.emptyDataset(Encoders.bean(Employee.class));
		ds3.show();		
		/*  result:
		 	+---+--------+------+
			|eid|jobTitle|salary|
			+---+--------+------+
			+---+--------+------+

		*/
		
		//add a new column
		Dataset<Row> ds4 = ds2.withColumn("test_c1", upper(lit("a")));
		ds4.show();
		/*	result:
		  	+---+--------+------+-------+
			|eid|jobTitle|salary|test_c1|
			+---+--------+------+-------+
			|001|engineer| 10000|      A|
			|002|engineer| 30000|      A|
			|003|director| 50000|      A|
			|004|manager | 70000|      A|
			|005|chairman| 90000|      A|
			|007|chairman|100000|      A|
			+---+--------+------+-------+
		 */
		
		
		//remove a column
		ds4 = ds4.drop("eid");
		ds4.show();
		/* 	result:
		  	+--------+------+-------+
			|jobTitle|salary|test_c1|
			+--------+------+-------+
			|engineer| 10000|      A|
			|engineer| 30000|      A|
			|director| 50000|      A|
			|manager | 70000|      A|
			|chairman| 90000|      A|
			|chairman|100000|      A|
			+--------+------+-------+
		 */
		
		//remove column for the same value
		ds4=ds4.dropDuplicates("jobTitle");
		ds4.show();
		/* 	result:
			+--------+------+-------+
			|jobTitle|salary|test_c1|
			+--------+------+-------+
			|director| 50000|      A|
			|manager | 70000|      A|
			|engineer| 10000|      A|
			|chairman| 90000|      A|
			+--------+------+-------+
		 */
		
		
		
		
		
		
		/*spark.read().csv("");
		spark.read().text("");
		spark.read().textFile("");
		spark.read().jdbc(url, table, properties);
		spark.read().orc("");
		spark.read().parquet("");
		spark.read().table("").select("").explain();
		
		
		
		ds3.createOrReplaceTempView(viewName);
		ds3.createTempView(viewName);
		
		ds3.printSchema();
		
		ds3.take(n);
		ds3.takeAsList(n);		
		ds3.limit(n);
		ds3.first();
		ds3.show(n);
		ds3.foreach(func);
		ds3.foreachPartition(func);
		
		ds3.alias("");
		ds3.as("");
		
		
		ds3.select(cols);
		ds3.selectExpr(exprs)
		ds3.selectUntyped(columns)
		ds3.where(condition);
		ds3.sort(sortExprs);
		ds3.groupBy(cols);
		ds3.groupByKey(func, evidence$4);
		ds3.orderBy(sortExprs);
		ds3.join(right, usingColumns, joinType);
		ds3.apply(colName);
		ds3.filter(condition);
		
		ds3.agg(exprs);		
		ds3.distinct();
		ds3.count();
		
		ds3.unpersist();
		ds3.cache();
		
		ds3.col("");
		ds3.columns();
		ds3.withColumn(colName, col)
		ds3.drop("");
		ds3.dropDuplicates("");
		
		
		ds3.collectAsList();
		ds3.collect();
		ds3.toDF();
		ds3.toJavaRDD();
		ds3.toJSON();
		ds3.toLocalIterator()
		ds3.toPythonIterator()
		ds3.javaToPython()
		
		ds3.map(func, evidence$7);
		ds3.mapPartitions(func, evidence$8);
		ds3.flatMap(f, encoder);
		
		ds3.isLocal();
		ds3.isStreaming();
		
		ds3.write().csv(path);
		ds3.write().orc("");
		ds3.write().jdbc(url, table, connectionProperties);
		ds3.write().json(path);
		ds3.write().text(path);
		ds3.write().parquet(path);
		ds3.write().save();
		ds3.write().saveAsTable("");
		ds3.write().insertInto("");*/
		
		//example1(ds_employee,ds_employeedep);
		//example2(ds_employee,ds_employeedep);
		//example3(ds_employee,ds_employeedep);
		
										
	}
	
	public static void example1(Dataset ds_employee,Dataset ds_employeedep ){
		Dataset<Row> result = ds_employeedep.select(col("name"),col("age"),col("age").geq(35),col("age").equalTo(35),col("age").leq(35),col("age").$eq$bang$eq(35));
		result.show();
		
		result = ds_employeedep.select(col("name"),col("age"),col("age").between(30, 46));
		result.show();
		
		System.out.println("age>=30 and age <=40 and name like '%ra%'");
		result = ds_employeedep.where(col("age").geq(30).and(col("age").leq(40).and(col("name").like("%ra%"))));
		result.show();
		
		result = ds_employeedep.groupBy(ds_employeedep.col("age")).agg(count("age"));
		result.show();
		
		result = ds_employeedep.agg(sum("age"),count("age"),avg("age"),max("age"),min("age"));		
		result.show();
		
		result = ds_employee.join(ds_employeedep,ds_employee.col("eid").equalTo(ds_employeedep.col("eid")));
		result.show();
		
		result = ds_employee.join(ds_employeedep,ds_employee.col("eid").equalTo(ds_employeedep.col("eid"))).drop(ds_employeedep.col("eid"));
		result.show();
		
		result = ds_employee.join(ds_employeedep,ds_employee.col("eid").equalTo(ds_employeedep.col("eid")))				 
				 .groupBy("age").agg(count("age"),sum("salary"),max("salary"),min("salary"));
		result.show();											
	}
	
	public static void example2(Dataset ds_employee,Dataset ds_employeedep ){
		Dataset<Row> result = ds_employee.withColumn("test_c1",floor("salary"));
		result.show();
	}
	
	//以下為Dataset join範例
	public static void example3(Dataset ds_employee,Dataset ds_employeedep ){
		
		System.out.println("Non key join:");
		Dataset<Row> result_1 = ds_employee.join(ds_employeedep);
		result_1.show();
		
		System.out.println("key join:");
		Dataset<Row> result_2 = ds_employee.join(ds_employeedep,ds_employee.col("eid").equalTo(ds_employeedep.col("eid")));
		result_2.show();
		
		/*
		 * join type:
		 * 		inner/full/outer/left/leftouter/leftsemi/right/rightouter
		 */
		System.out.println("left join:");
		Dataset<Row> result_3 = ds_employee.join(ds_employeedep,ds_employee.col("eid").equalTo(ds_employeedep.col("eid")),"left");
		result_3.show();
		
		System.out.println("left outer join:");
		Dataset<Row> result_3_1 = ds_employee.join(ds_employeedep,ds_employee.col("eid").equalTo(ds_employeedep.col("eid")),"leftouter");
		result_3_1.show();
		
		System.out.println("left semi join:");
		Dataset<Row> result_3_2 = ds_employee.join(ds_employeedep,ds_employee.col("eid").equalTo(ds_employeedep.col("eid")),"leftsemi");
		result_3_2.show();
		
		System.out.println("right join:");
		Dataset<Row> result_4 = ds_employee.join(ds_employeedep,ds_employee.col("eid").equalTo(ds_employeedep.col("eid")),"right");		
		result_4.show();
		
		System.out.println("right outer join:");
		Dataset<Row> result_4_1 = ds_employee.join(ds_employeedep,ds_employee.col("eid").equalTo(ds_employeedep.col("eid")),"rightouter");		
		result_4_1.show();
		
		System.out.println("inner join");
		Dataset<Row> result_5 = ds_employee.join(ds_employeedep,ds_employee.col("eid").equalTo(ds_employeedep.col("eid")),"inner");
		result_5.show();
		
		System.out.println("full join:");
		Dataset<Row> result_6 = ds_employee.join(ds_employeedep,ds_employee.col("eid").equalTo(ds_employeedep.col("eid")),"full");
		result_6.show();
		
		System.out.println("outer join:");
		Dataset<Row> result_7 = ds_employee.join(ds_employeedep,ds_employee.col("eid").equalTo(ds_employeedep.col("eid")),"outer");
		result_7.show();
		
		//刪除重複的Field
		result_5 = result_5.drop(ds_employeedep.col("eid"));
		result_5.printSchema();
		
		result_5.write().mode(SaveMode.Overwrite).saveAsTable("empresult");
	}
}
