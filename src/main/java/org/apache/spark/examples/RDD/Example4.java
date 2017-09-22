package org.apache.spark.examples.RDD;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.asc;
import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.ascii;
import static org.apache.spark.sql.functions.*;
public class Example4 {
	public static void main(String[] args){
		SparkSession spark = SparkSession.builder().appName("RDD.Example4").enableHiveSupport().getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		
		List<Item> list = Arrays.asList(
				new Item("Name3","No2","A",30),
				new Item("Name2","No2","A",10),
				new Item("Name1","No1","A",50),
				new Item("Name1","No2","B",30),
				new Item("Name2","No1","B",40),
				new Item("Name2","No1","B",80),
				new Item("Name1","No3","A",10)
		 );
		
		Dataset<Row> ds = spark.createDataFrame(jsc.parallelize(list),Item.class);
		
		ds.groupBy(col("itemName"),col("itemNo")).agg(sum("Total").as("sum")).orderBy(asc("itemName"),asc("itemNo")).show();
		
		ds.groupBy(col("itemName"),col("itemNo")).agg(sum("Total").as("sum")).orderBy(desc("sum")).show();
				
		ds.select(col("itemsource").as("source"),ascii(col("itemsource")).as("ascii")).show();
		
		List<Item> list2 = Arrays.asList(new Item("Name1","No3","A",10));
		
		Dataset<Row> ds2 = spark.createDataFrame(list2, Item.class);
		
		ds2.printSchema();
		ds2.select(current_date()).show();
		ds2.select(current_timestamp()).show();		
	}

	
}
