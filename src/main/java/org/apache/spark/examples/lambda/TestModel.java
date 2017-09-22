package org.apache.spark.examples.lambda;

public class TestModel {
	String name="";
	String age="";
	String gender="";
	int salary=0;
	public TestModel(String name,String gender,String age,int salary){
		this.name=name;
		this.age=age;
		this.gender=gender;
	    this.salary=salary;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getAge() {
		return age;
	}
	public void setAge(String age) {
		this.age = age;
	}
	
	public int getSalary() {
		return salary;
	}
	public void setSalary(int salary) {
		this.salary = salary;
	}
	public String getGender() {
		return gender;
	}
	public void setGender(String gender) {
		this.gender = gender;
	}
	
}

