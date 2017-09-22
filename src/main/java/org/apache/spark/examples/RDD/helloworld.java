package org.apache.spark.examples.RDD;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec._;
import org.apache.spark.*;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class helloworld 
{	  
		
		private static Function<Tuple2<String,Integer>, String> func = new Function<Tuple2<String,Integer>, String>() { // Function to get names.
			@Override
			public String call(Tuple2<String,Integer> arg0) throws Exception {
				return arg0._1;
			}
		};
		
    	public static void main(String[] args) throws Exception {
    		
		  System.out.println("			||||||||||||||||||||Starting program......||||||||||||||||||||||||||||||");		  
		  SparkSession spark = SparkSession.builder().appName("RDD.Example1").getOrCreate();
		  spark.sparkContext().setLogLevel("ERROR");		  
		  JavaRDD<String> file = spark.read().textFile(args[0]).toJavaRDD(); // read the input text file specified in args[0]
		  JavaRDD<String> words = file.flatMap(data->Arrays.asList(data).iterator());	
		  
		  JavaPairRDD<Integer,String> pairs = words.mapToPair(new PairFunction<String, Integer,String>() { // get the tokens in each line
		    public Tuple2<Integer,String> call(String s) {                                                 // here seperated by " "
		    	
		    	 StringTokenizer st = new StringTokenizer(s); // default delimiter is space.
		    	 String name = ""; int salary = 0;
		    	 while(st.hasMoreElements()) // while there are more lines to process
				 {
					  name = st.nextElement().toString(); // get the name from the line
					  salary = Integer.parseInt(st.nextElement().toString()); // get the salary from the line
				 }
		    	 return new Tuple2<Integer,String>(salary,name); // make a key-value pair 
		    }
		  }); 
		  
		  // The following function is done so that we get the names as KEY.
		  JavaPairRDD<String,Integer> pairs2 = words.mapToPair(new PairFunction<String, String,Integer>() { // get the tokens in each line
			    public Tuple2<String,Integer> call(String s) {
			    	
			    	 StringTokenizer st = new StringTokenizer(s);
			    	 String name = ""; int salary = 0;
			    	 while(st.hasMoreElements())
					 {
						  name = st.nextElement().toString();
						  salary = Integer.parseInt(st.nextElement().toString());
					 }
			    	 return new Tuple2<String,Integer>(name,salary);
			    }
			  }); 
		  
		  
		  JavaPairRDD<String, Integer> finalList = pairs2.sortByKey(true); // sort according to names.
		  
		  JavaPairRDD<Integer, String> answer = pairs.sortByKey(false); // sort descending for maximum
		  List<Tuple2<Integer, String>> t2 = answer.take(1); // take first element from descending for maximum
		  JavaPairRDD<Integer, String> answer2 = pairs.sortByKey(true); // sort ascending for minimum
		  List<Tuple2<Integer, String>> t = answer2.take(1); // take first element from ascending for minimum
		  
		  System.out.println("\n\nMaximum salary is by "+ t2.get(0)._2 + " and maximum salary is "+t2.get(0)._1);
		  System.out.println("\n\nMinimum salary is by "+ t.get(0)._2 + " and minimum salary is "+t.get(0)._1+"\n\n");
		  System.out.println(finalList.groupBy(func).sortByKey(false).collect());   // for grouping by "name"
		  System.out.println(finalList.groupBy(func).sortByKey(true).collect());
		  //finalList.saveAsTextFile(args[1]);  // save to text file(s)
	  } 
	 
}