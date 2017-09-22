package org.apache.spark.examples.RDD;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
class DummyComparatorInt implements Serializable,Comparator<Integer> {
	public int compare(Integer o1, Integer o2) {
	    return o1-o2;
	}
}

public class Example3 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkSession spark = SparkSession.builder().appName("RDD.Example3").getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
				
		List<String> liststr = Arrays.asList("apple","orange","apple","apple","cccc");
		JavaRDD<String> rddstr = jsc.parallelize(liststr);
		
		//Map Example:
		List<Integer> list1 = Arrays.asList(1,2,3,4,5,1,4,7,9,8);		
		JavaRDD<Integer> rdd1 = jsc.parallelize(list1);
		System.out.println("Map Result:"+rdd1.map(data->data*10).collect());
		
		
		//flatMap Example
		List<Integer> list2 = Arrays.asList(5,6,4,3,1);		
		JavaRDD<Integer> rdd2 = jsc.parallelize(list1);
		System.out.println("flatMap Result:"+rdd1.flatMap(data1->{			
			return Arrays.asList(data1,data1*100,data1*10).iterator();
		}).collect());
						
		List<Integer> list3 = Arrays.asList(1, 2, 4, 3, 5, 6, 7);
		JavaRDD<Integer> rdd3 = jsc.parallelize(list3,2);				
		System.out.println("mapPartitions Result:"+rdd3.mapPartitions(data1->{
			int isum = 0;
	        while(data1.hasNext())
	            isum += data1.next();
	        List<Integer> list = new ArrayList<Integer>();
	        list.add(isum);
	        return list.iterator();
		}).collect());
				
		System.out.println("mapPartitions Result:"+rdd3.mapPartitionsWithIndex((data1,data2)->{
			 List<String> list = new ArrayList<String>();
			 int i = 0;        
			  while (data2.hasNext())           
				  list.add(Integer.toString(data1) + "|" + data2.next().toString() + Integer.toString(i++));        
			  return list.iterator();
		},false).collect());
				
		System.out.println("getNumPartitions Example Result:"+rdd3.getNumPartitions());
				
		System.out.println("Filter Result:"+rdd1.filter(data1->data1%5==0).collect());
								
		System.out.println("distinct Result:"+rdd1.distinct().collect());
			
		List<Integer> list4 = IntStream.range(0,10).boxed().collect(Collectors.toList());
		JavaRDD<Integer> rdd4 = jsc.parallelize(list4);
		System.out.println("Sample Example Result:"+rdd4.sample(false,0.5).collect());
				
		System.out.println("TakeSample Example Result:"+rdd4.takeSample(false,4));
							
		System.out.println("union Example Result:"+rdd1.union(rdd2).collect());
		
		System.out.println("intersection Example Result:"+rdd1.intersection(rdd2).collect());
				
		System.out.println("SortBy Example Result:"+rddstr.sortBy(data1->data1.charAt(0),true,3).collect());
						
		System.out.println("glom Example Result:"+rdd1.glom().collect());
				
		System.out.println("cartesian Example Result:"+rddstr.cartesian(rdd1).collect());
				
		System.out.println("groupby Example Result:"+rddstr.groupBy(word -> word.charAt(0)).collect());
		
		//System.out.println(rdd2.pipe("ls -la").collect());
				
		System.out.println("reduce result:"+rdd1.reduce((x,y)->x+y));
		System.out.println("fold result:"+rdd1.fold(1,(x,y)->x+y));
		System.out.println("max result:"+rdd1.max(new DummyComparatorInt()));
		System.out.println("min result:"+rdd1.min(new DummyComparatorInt()));
		System.out.println("rdd1 size:"+rdd1.count());
		
		System.out.println("-------Top Example Result:-------");
		System.out.println("Top 5 result:"+rdd1.top(5));
		System.out.println("First result:"+rdd1.first());
		
		List<Integer> list5 = Arrays.asList(7,2,3,6,5,1,4,7,9,8);	
		JavaRDD<Integer> rdd5 = jsc.parallelize(list5);
		System.out.println("List5 Data:"+list5.stream().collect(Collectors.toList()));
		System.out.println("take result:"+rdd5.take(5));
		System.out.println("takeOrder result:"+rdd5.takeOrdered(5));
		System.out.println("countByValue result:"+rdd5.countByValue());
		
		System.out.println(rdd5.coalesce(5).collect());
		
		List<Integer> list6 = Arrays.asList(1,2,3,4,5,6);
		JavaRDD<Integer> rdd6 = jsc.parallelize(list6);
		List<String> list7 = Arrays.asList("apple","banala","orange","Lemon","aaa","bbb");		
		JavaRDD<String> rdd7 = jsc.parallelize(list7);
		List<Integer> list8 = Arrays.asList(100,300,68,70,90,100);
		JavaRDD<Integer> rdd8 = jsc.parallelize(list8);
		System.out.println("rdd5&rdd1 zip Result:"+rdd6.zip(rdd7).zip(rdd8).collect());
		
		//System.out.println(rdd2.pipe("grep -i \"A\"").collect()+"\n");
		
		JavaRDD<String>  rdd9= rdd6.zipPartitions(rdd7,(data1,data2)->{
			LinkedList<String> linkedList = new LinkedList<String>();        
	        while(data1.hasNext() && data2.hasNext())            
	            linkedList.add(data1.next().toString() + "_" + data2.next().toString());        
	        return linkedList.iterator();	
		});
		System.out.println("zipPartition RDD Result:"+rdd9.collect());
		
		System.out.println("zipWithIndex    Result:"+rdd7.zipWithIndex().collect());
		System.out.println("zipWithUniqueId Result:"+rdd7.zipWithUniqueId().collect());
				
		System.out.println(rdd1.getStorageLevel().memoryMode());
		System.out.println("debug string:"+rdd1.toDebugString());
		System.out.println("rdd1 getNumPartitions:"+rdd1.getNumPartitions());
		rdd1.repartition(1);
		System.out.println("rdd1 getNumPartitions:"+rdd1.getNumPartitions());
		System.out.println("rdd1 rdd:"+rdd1.rdd().collect());
		
		
		
		
	}

}
