package org.apache.spark.examples.lambda;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Example2 {
	public static void main(String args[]){
		Map<String,String> map = new HashMap<String,String>();
		map.put("test1","10000");
		map.put("test2","20000");
		map.put("test3","30000");
		map.put("test4","40000");
		
		System.out.println("透過keySet,取得map資料(傳統寫法)");
		System.out.println("-------------------------");
		Set<String> keySet = map.keySet();
		for(String k:keySet){
			System.out.println(k+":"+map.get(k));
		}
		
		System.out.println("");
		System.out.println("透過Lambda,取得map資料");
		System.out.println("-------------------------");
		map.forEach((k,v) -> System.out.println(k + ":" + v));
		
		List<TestModel> list = new ArrayList<TestModel>();
		
		
		//for Rojer
		TestModel model = new TestModel("test1","F","20",10000);list.add(model);
		model = new TestModel("test2","F","30",20000);list.add(model);
		model = new TestModel("test3","F","20",30000);list.add(model);
		model = new TestModel("test4","M","30",40000);list.add(model);
		model = new TestModel("test5","M","20",50000);list.add(model);
		
		Map<String, Map<String, Integer>> amp1  =list.stream().collect(
				//Collectors.groupingBy(x -> return new ArrayList<String>(Arrays.asList(TestModel.getGender(), TestModel.getAge())), Collectors.toCollection(LinkedHashSet::new))
					Collectors.groupingBy(TestModel::getGender,
					Collectors.groupingBy(TestModel::getAge,Collectors.summingInt(TestModel::getSalary))
				)
		);
		amp1.forEach((key,value)->{	         
	         value.forEach((key2,value2)->{
	             System.out.println(key+","+key2+",value2:"+value2);
	         });
		});
		
			  
		
		Map<String, Map<String, List<TestModel>>> amp2  =list.stream().collect(
				//Collectors.groupingBy(x -> return new ArrayList<String>(Arrays.asList(TestModel.getGender(), TestModel.getAge())), Collectors.toCollection(LinkedHashSet::new))
					Collectors.groupingBy(TestModel::getGender,
					Collectors.groupingBy(TestModel::getAge)
				)
		);		
		amp2.forEach((key,value)->{	         
	         value.forEach((key2,value2)->{
	        	 for(int i=0;i<value2.size();i++){
	        		 System.out.println(key+","+key2+",TestModel:"+value2.get(i).getSalary());
	        	 }
	             
	         });
		});
		
		//group by price
        Map<String, List<TestModel>> groupByPriceMap = list.stream().collect(Collectors.groupingBy(TestModel::getGender));
        
        

        System.out.println(groupByPriceMap);

		// group by price, uses 'mapping' to convert List<Item> to Set<String>
        Map<String, List<String>> result =
                list.stream().collect(
                        Collectors.groupingBy(TestModel::getGender,
                                Collectors.mapping(TestModel::getAge, Collectors.toList())
                        )
                );

        System.out.println(result);
		
	}
}
