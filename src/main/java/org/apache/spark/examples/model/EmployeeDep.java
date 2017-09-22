package org.apache.spark.examples.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class EmployeeDep implements Serializable{
	private String eid="";
	private String name="";
	private String gender="F";
	private int age=0;
	private List<String> relative = new ArrayList<String>();
	public EmployeeDep(String eid,String name,String gender,int age,List<String> relative){
		this.eid=eid;
		this.name=name;
		this.gender=gender;
		this.age=age;
		this.relative=relative;
	}
	public String getEid() {
		return eid;
	}
	public void setEid(String eid) {
		this.eid = eid;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getGender() {
		return gender;
	}
	public void setGender(String gender) {
		this.gender = gender;
	}
	public int getAge() {
		return age;
	}
	public void setAge(int age) {
		this.age = age;
	}
	public List<String> getRelative() {
		return relative;
	}
	public void setRelative(List<String> relative) {
		this.relative = relative;
	}
	
}
