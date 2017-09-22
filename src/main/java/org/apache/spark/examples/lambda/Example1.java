package org.apache.spark.examples.lambda;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class Example1 {
	//List Lambda用法
	public static void main(String args[]){
		List<String> list = new ArrayList<String>();
		list.add("test1");
		list.add("test2");
		list.add("test3");
		list.add("test4");
		
		System.out.println("for迴圈使用index的方式取值");
		System.out.println("---------------------");
		for(int i=0;i<list.size();i++){
			System.out.println("data:"+list.get(i));
		}
		
		System.out.println("for迴圈使用iterator的方式取值");
		System.out.println("---------------------");
		for(String str:list){
			System.out.println("data:"+str);
		}
		
		System.out.println("for迴圈使用foreach+Lambda的方式取值");
		System.out.println("---------------------");
	
		
		list.forEach(s -> System.out.println("data:"+s));
		
		System.out.println("for迴圈使用foreach+Lambda的方式取值");
		System.out.println("---------------------");
		list.forEach(System.out::println);
		
		
	}
}
