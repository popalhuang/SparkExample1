package org.apache.spark.examples.lambda;

import java.util.stream.Collectors.*;
import java.util.stream.*;
import java.util.*;
import java.util.Map.Entry;

 
public class lambdaExample {

    public static void main(String[] args) {
        Run();
    }
 
    static private void Run(){
    	
        //initial data
    	
        System.out.println("<--List1--> initData");
        List<Item> list_1 = InitData();
        for (Item it : list_1) {
            System.out.println(it.getItemName() + "," + it.getItemNo() + "," + it.getItemSource());
        }
        System.out.println("-----------");
        Comparator<Item> comparator = Comparator.comparing(Item::getItemName).thenComparing(Item::getItemNo);        
        List<Item> l = list_1.stream().sorted(comparator).collect(Collectors.toList());
        
        Comparator<Item> comparator2= Comparator.comparing(Item::getItemNo);
        l.forEach(data->System.out.println(data.getItemName()+","+data.getItemNo()));
        
        System.out.println("-----------");	
       Map<String, Map<String, List<Item>>> map = l.stream().sorted(comparator).collect(        			
					Collectors.groupingBy(Item::getItemName,Collectors.groupingBy(Item::getItemNo,Collectors.toList())            
				)
		);          
        //System.out.println(list.stream().collection(Collectors.toList()));      
        
       for (Entry<String, Map<String, List<Item>>> entry : map.entrySet()) {
    	   List<List<Item>> list_item = entry.getValue().values().stream().collect(Collectors.toList());    	   
    	   for(List<Item> li : list_item){    	  
    		   for(Item item:li){
    			   System.out.println(item.getItemName()+","+item.getItemNo());
    		   }
    	   }    	          
        }
    }
   
    
     
    static private List<Item> InitData(){
        List<Item> list = Arrays.asList(
			new Item("Name2","No2","A",10),
			new Item("Name1","No1","A",50),
			new Item("Name1","No2","B",30),
			new Item("Name2","No1","B",40),
			new Item("Name2","No1","B",80),
			new Item("Name1","No3","A",10)
        );
        return list;
    }
}

