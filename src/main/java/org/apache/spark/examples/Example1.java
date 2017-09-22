package org.apache.spark.examples;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class Example1 {

	public static void main(String[] args) {
		
		// TODO Auto-generated method stub		
		String path = args[0];	//取得文件路徑(for HDFS,hdfs://mna1:8020/xxx)
		
		SparkSession spark = SparkSession.builder().appName("SparkExample1").getOrCreate();
		
		//讀取指定路徑下相關
		JavaRDD<String> rdd1 = spark.read().textFile(path).toJavaRDD();
		
		JavaRDD<String> rdd2 = rdd1.flatMap(s -> Arrays.asList(s.split(",")).iterator());
		
		JavaPairRDD<String, Integer> prdd3 = rdd2.mapToPair(word -> new Tuple2<>(word, 1)); 
					    
		JavaPairRDD<String, Integer> counts = prdd3.reduceByKey((a, b) -> a + b);
		
		System.out.println(counts.collect());						
	}

}
