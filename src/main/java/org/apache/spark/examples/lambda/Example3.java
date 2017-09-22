package org.apache.spark.examples.lambda;

import java.util.ArrayList;
import java.util.List;

class Human {
    private String name;
    private int age;
 
    public Human() {
        super();
    }
 
    public Human(final String name, final int age) {
        super();
 
        this.name = name;
        this.age = age;
    }

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}
 
    // standard getters and setters    
	
}

public class Example3 {
	public static void main(String args[]){
		List<Human> humans = new ArrayList<Human>();
		humans.add(new Human("test1",10));
		humans.add(new Human("test2",20));
		humans.add(new Human("test3",15));
		
		
		for(Human man:humans){
			System.out.println(man.getName()+":"+man.getAge());
		}
		
		System.out.println("");
		System.out.println("以Age屬性排序(升冪)");	
		System.out.println("--------------------");		
		humans.sort((Human h1, Human h2) -> h1.getAge()-h2.getAge());		
		humans.forEach(human -> System.out.println(human.getName()+":"+human.getAge()));		
		System.out.println("以Age屬性排序(降冪)");		
		System.out.println("--------------------");		
		humans.sort((Human h1, Human h2) -> h2.getAge()-h1.getAge());		
		humans.forEach(human -> System.out.println(human.getName()+":"+human.getAge()));
		
		System.out.println("");
		System.out.println("以Name屬性排序(升冪)");
		System.out.println("--------------------");		
		humans.sort((Human h1, Human h2) -> h1.getName().compareTo(h2.getName()));
		humans.forEach(human -> System.out.println(human.getName()+":"+human.getAge()));
		System.out.println("以Name屬性排序(降冪)");
		System.out.println("--------------------");		
		humans.sort((Human h1, Human h2) -> h2.getName().compareTo(h1.getName()));
		humans.forEach(human -> System.out.println(human.getName()+":"+human.getAge()));
		
		
	}
}
