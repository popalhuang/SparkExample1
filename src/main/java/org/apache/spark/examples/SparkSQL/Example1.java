package org.apache.spark.examples.SparkSQL;

import static org.apache.spark.sql.functions.col;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.COL;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;
import scala.Tuple4;
import scala.collection.Parallelizable;

class User implements Serializable{
	
	private String name="";
	private String location="";
	private int age = 0;
	private double salary=0.0;
	
	public User(String name,String location,int age,double salary){
		this.name = name;
		this.location = location;
		this.age = age;
		this.salary = salary;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getLocation() {
		return location;
	}
	public void setLocation(String location) {
		this.location = location;
	}
	public int getAge() {
		return age;
	}
	public void setAge(int age) {
		this.age = age;
	}
	public double getSalary() {
		return salary;
	}
	public void setSalary(double salary) {
		this.salary = salary;
	}
	
	
}
public class Example1 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkSession spark = SparkSession.builder().appName("SparkSQL.Example1").enableHiveSupport().getOrCreate();
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		spark.sparkContext().setLogLevel("ERROR");
		//HDFS File(CSV->DataFrame)
		Dataset<Row> ds = spark.read().csv("/user/hadoop/data/dataframe_1.csv");
		
		ds = ds.withColumnRenamed("_c0", "name");
		ds = ds.withColumnRenamed("_c1", "location");
		ds = ds.withColumnRenamed("_c2", "age");
		ds = ds.withColumnRenamed("_c3", "salary");		
		ds.printSchema();
		ds.show();
		
		Dataset<Row> ds2 = ds.select(col("name"),col("age"),col("age").plus(10),col("age").minus(7),col("age").divide(5),col("age").multiply(10));
		ds2.show();
		
		Dataset<Row> ds3 = ds.select(col("name"),col("age"),col("age").geq(35),col("age").equalTo(35),col("age").leq(35),col("age").$eq$bang$eq(35));
		ds3.show();
		
		Dataset<Row> ds4 = ds.select(col("name"),col("age"),col("age").between(30, 46));
		ds4.show();
		
		System.out.println("age>=35 and age <=45 and name like '%cc%'");
		Dataset<Row> ds5 = ds.where(col("age").geq(35).and(col("age").leq(45).and(col("name").like("%cc%"))));
		ds5.show();
								
		//List to DataFrame
		List<Row> data2 = Arrays.asList(  
			RowFactory.create("Alex","Taipei",39,230.00),  
			RowFactory.create("Bob","Kaohus",40,290.00),  
			RowFactory.create("Chris","Taina",38,300.00),
			RowFactory.create("Dave","NewYork",71,340.00),
			RowFactory.create("Kevin","Korea",20,330.00)
		); 				
				
		//Create StructFields
		List<StructField> structFields = new ArrayList<StructField>();
		structFields.add(DataTypes.createStructField("name",DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("location",DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("age",DataTypes.IntegerType, true));
		structFields.add(DataTypes.createStructField("salary",DataTypes.DoubleType, true));		
		StructType schema = DataTypes.createStructType(structFields);				
		
		Dataset<Row> ds6 = spark.createDataFrame(data2, schema);		
		ds6.printSchema();
		ds6.show();
		ds6.write().mode(SaveMode.Overwrite).saveAsTable("employee");
		
		//From Hive Table get DataSet
		String sql_1= "select * from employee";
		System.out.println(sql_1);
		Dataset<Row> ds7 = spark.sql(sql_1);
		ds7.show();
		
		String sql_2= "age>=20 and age <=30";
		System.out.println("hive_sql=select * from employee where age >=20 and age<=30 order by age asc,salary desc limit 10;");
		System.out.println("----------");		
		ds7 = ds7.select(col("eid"),col("name"),col("age"),col("salary"))
				 .where(sql_2)
				 .sort(col("age").asc(),col("salary").desc());
		ds7.show(10);
		
			
		String sql_3 = "select age,count(age) from employee group by age";
		ds7 =ds7.select(col("age")).groupBy(col("age")).count();
		ds7.show();
		
		//分區統計範例(以年齡區間當group by條件)		
		String sql_4 = "select concat(cast(floor(age/10)*10 as string),'~',cast(floor(age/10)*10+(10-1) as string)) as range1,count(*) as c from employee group by floor(age/10) order by range1";
		System.out.println("spark_sql:"+sql_4);
		System.out.println("----------");
		Dataset<Row> ds8 = spark.sql(sql_4);
		ds8.printSchema();
		ds8.show();
		
		//分區統計範例(以年/月區間當group by條件)
		String sql_5 = "select substring(birthday,0,7) as month,count(*) from employee where birthday between '2015' and '2016' group by substring(birthday,0,7) order by month";		
		System.out.println("spark_sql:"+sql_5);
		System.out.println("----------");
		Dataset<Row> ds9 = spark.sql(sql_5);
		ds9.printSchema();
		ds9.show();
		
		
	}

}
