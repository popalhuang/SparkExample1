package org.apache.spark.examples.RDD;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;

public class Example2 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkSession spark = SparkSession.builder().appName("RDD.Example2").getOrCreate();		
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		
		List<String> names = Arrays.asList("xurunyun","liangyongqi","wangfei");
        JavaRDD<String> nameRDD = jsc.parallelize(names);

        final Map<String, Integer> scoreMap = new HashMap<String, Integer>();
        scoreMap.put("xurunyun", 150);
        scoreMap.put("liangyongqi", 100);
        scoreMap.put("wangfei", 90);
             
        JavaRDD<Integer> scoreRDD = nameRDD.mapPartitions(data->{
        	List<Integer> list = new ArrayList<Integer>();

            while(data.hasNext()){
                String name = data.next();
                System.out.println("name:"+name);
                Integer score = scoreMap.get(name);
                list.add(score);
            }
            return list.iterator();            
        });
        
        scoreRDD.foreach(data->System.out.println(data));
        

        JavaRDD<String> nameRDD2 = jsc.parallelize(names,20);
        JavaRDD<Integer> scoreRDD2 = nameRDD2.mapPartitionsWithIndex((index,data2)->{
        	List<Integer> list2 = new ArrayList<Integer>();        	
            while(data2.hasNext()){
                String name = data2.next();
                System.out.println(index + " : " + name);               
                Integer score = scoreMap.get(name);
                list2.add(score);
            }
            return list2.iterator();         
        }, true);
        scoreRDD2.foreach(data->System.out.println(data));
        
        
	}

}
