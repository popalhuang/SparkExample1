package org.apache.spark.examples.RDD;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.examples.RDD.Item;
import org.apache.spark.rdd.JdbcRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

class DummyComparator implements Serializable,Comparator<Tuple2<Integer, String>> {
	public int compare(Tuple2<Integer, String> o1, Tuple2<Integer, String> o2) {
	    return Integer.compare(o1._1(), o2._1());
	}
}
class Comp implements Comparator<String>,Serializable{    
	@Override    
    public int compare(String o1, String o2) {            
		return o1.toLowerCase().compareTo(o2.toLowerCase());    
  }
}

public class Example1 {
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		for(int i=0;i<args.length;i++){
			System.out.println("param:"+args[i]);
		}
		
		System.out.println("JavaRDD Example Execute Result:");
		System.out.println("--------------------");
		
		//Spark Init		
		SparkSession spark = SparkSession.builder().appName("RDD.Example1").getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		
		
		//prepare materials
		List<String >list1 = Arrays.asList("apple", "banana", "strawberry", "lemon", "apple", "banana", "grapes", "watermelon", "strawberry", "lemon", "lemon");				  
		list1.sort((data1,data2)->data1.compareTo(data2));
		list1.forEach(System.out::println);
		
		//Topic1:Count the number of occurrences of each Item in the List(use reduceByKey)
		JavaRDD<String> rdd =  jsc.parallelize(list1);		
		JavaPairRDD<String, Integer> fruitinit = rdd.mapToPair(data->new Tuple2<String, Integer>(data, 1));		
		JavaPairRDD<String, Integer> counts = fruitinit.reduceByKey((x,y)->x+y);		
		System.out.println("use reduceByKey:"+counts.collect());
		
		//use GroupByKey
		System.out.println("use groupByKey:"+fruitinit.groupByKey().map(data->new Tuple2(data._1,data._2)).collect());
		
		//sum up the fruits' values
		int fruitsum= counts.values().reduce((a,b)->a+b);
		System.out.println("use counts.values().reduce:"+fruitsum);
		
		//return an RDD with the keys of each tuple
		System.out.println("use counts.keys().collect:"+counts.keys().collect());
		
		//使用map將RDD中List下所有的資料全部+s
		System.out.println("use map in word add s:"+rdd.map(data->data+"s").collect());
		System.out.println("use flatmap in word add s:"+rdd.flatMap(data->Arrays.asList(data+"s").iterator()).collect());
		
		System.out.println("ap like filter"+rdd.filter(data->data.indexOf("ap")>=0).collect());
		
		System.out.println("list is distinct:"+rdd.distinct().collect());
		
		System.out.println("list is take(2):"+rdd.take(2));
		
		//以第一個字母順序排列(a->z)
		System.out.println("First words sort(a->z):"+rdd.sortBy(data->data.substring(0), true, 3).collect());
		
		//以第一個字母順序排列(z->a)
		System.out.println("First words sort(z->a):"+rdd.sortBy(data->data.substring(0), false, 3).collect());
				
		//以下為reduce運算
		int sum = jsc.parallelize(Arrays.asList(10, 20, 30, 40, 50)).reduce((a,b)->a+b);
		System.out.println("Reduce num of sum:"+sum);
		
		System.out.println();
		System.out.println("JavaPairRDD Example Execute Result:");
		System.out.println("--------------------");
				
		List<Tuple2<String,Integer>> list2 = new ArrayList<Tuple2<String,Integer>>();
		list2.add(new Tuple2("apple",1));
		list2.add(new Tuple2("apple",2));
		list2.add(new Tuple2("banana",3));
		list2.add(new Tuple2("banana",3));
		list2.add(new Tuple2("barryraw",4));
		list2.add(new Tuple2("barryraw",8));		
		
		JavaPairRDD<String,Integer> pairrdd1 = jsc.parallelizePairs(list2,10);
		System.out.println("Total partitions:"+pairrdd1.getNumPartitions());
		System.out.println("JavaPairRDD Init Data:"+pairrdd1.collect());
		
		JavaPairRDD<String,Integer> result = pairrdd1.reduceByKey((x,y)->x+y);
		System.out.println("Result 1:"+result.collect());
		
		JavaPairRDD<Integer,String> result2 = result.mapToPair(data->new Tuple2<Integer,String>(data._2,data._1)); // make a key-value p
		System.out.println("Result 2:"+result2.collect());
				
		System.out.println("Result2 sortbykey asc :"+result.sortByKey(true).collect());
		System.out.println("Result2 sortbykey desc:"+result.sortByKey(false).collect());
		
		System.out.println("Result2 filter num > 10:"+result2.filter(data->data._1 > 10).collect());
		System.out.println("Result2 filter num < 10:"+result2.filter(data->data._1 < 10).collect());
		
		System.out.println("Result2 Max:"+result2.max(new DummyComparator())._2);
		System.out.println("Result2 Min:"+result2.min(new DummyComparator())._2);
		
		System.out.println("Result 3:"+result.aggregateByKey(0,(x,y)->y,(x,y)->x+y).collect());
		
		System.out.println("");
		System.out.println("JavaPairRDD Multiple Example:");
		System.out.println("------------");
		
		List<Tuple2<String,Integer>> list3 = new ArrayList<Tuple2<String,Integer>>();
		list3.add(new Tuple2("apple",3));
		list3.add(new Tuple2("apple",4));
		list3.add(new Tuple2("banana",7));		
		list3.add(new Tuple2("barryraw",1));
		list3.add(new Tuple2("barryraw",3));
		
		JavaPairRDD<String,Integer> pairrdd2 = jsc.parallelizePairs(list3);
		System.out.println("JavaPairRDD Init Data1:"+pairrdd1.collect());
		System.out.println("JavaPairRDD Init Data2:"+pairrdd2.collect());
		
		JavaPairRDD<String,Integer> pairrdd3 = pairrdd1.union(pairrdd2);
		System.out.println("JavaPairRDD pairrdd1 union pairrdd2:"+pairrdd3.collect());
		
		JavaPairRDD<String, Tuple2<Integer, Integer>> pairrdd4 = pairrdd1.join(pairrdd2);
		System.out.println("JavaPairRDD pairrdd1.join(pairrdd2):"+pairrdd4.collect());
		
		JavaPairRDD<String, Tuple2<Optional<Integer>, Optional<Integer>>> pairrdd5 = pairrdd1.fullOuterJoin(pairrdd2);
		System.out.println("JavaPairRDD pairrdd1.fullOuterJoin(pairrdd2):"+pairrdd5.collect());
		
		JavaPairRDD<String, Tuple2<Integer, Optional<Integer>>> pairrdd6 = pairrdd1.leftOuterJoin(pairrdd2);
		System.out.println("JavaPairRDD pairrdd1.leftOuterJoin(pairrdd2):"+pairrdd6.collect());
		
		JavaPairRDD<String, Tuple2<Optional<Integer>,Integer>> pairrdd7 = pairrdd1.rightOuterJoin(pairrdd2);
		System.out.println("JavaPairRDD pairrdd1.rightOuterJoin(pairrdd2):"+pairrdd7.collect());
		
		
		//Example
		List<Item> list = Arrays.asList(
			new Item("Name3","No2","A",30),
			new Item("Name2","No2","A",10),
			new Item("Name1","No1","A",50),
			new Item("Name1","No2","B",30),
			new Item("Name2","No1","B",40),
			new Item("Name2","No1","B",80),
			new Item("Name1","No3","A",10)
	    );		
		
		JavaRDD<Item> jrdd = jsc.parallelize(list);				
		
		JavaPairRDD<String, Iterable<Item>> prdd = jrdd.groupBy(Item::getItemName);		
		
		JavaPairRDD<String,Integer> prdd2=  prdd.flatMapValues(data->{			
			List<Integer> result1 = new ArrayList<Integer>();
			Iterator<Item> it = data.iterator();
			while(it.hasNext()){
				Item item = (Item)it.next();
				int total = item.getTotal();				
				result1.add(total);				
			}
			return result1;
		});		
		prdd2.collect().forEach(System.out::println);
		JavaPairRDD<String, Integer> prdd3 = prdd2.reduceByKey((data1,data2)->{
			return data1+data2;			
		});
		
		prdd3.sortByKey().collect().forEach(System.out::println);
		
		
		
		
		
		//prdd.groupBy(prdd,Item::getItemNo);
	  //jrdd.groupBy(data->data.getItemName()).collect().forEach(data->System.out.println(data));
		
		
		//prdd.collect().forEach(System.out::println);
		//jrdd.sortBy(data->data.getTotal(), false, 3).collect().forEach(l->System.out.println(l.getTotal()));
		//jrdd.foreach(System.out::println);
		
		/*jrdd.groupBy(data->{
			List<Item> data1 = new ArrayList<Item>();			
			data.getItemName();
			return data1;
		}).collect().toString();*/
	}

}
