package org.apache.spark.examples.model;

import java.io.Serializable;

public class Employee implements Serializable {
	public String eid="";		
	public String jobTitle="";
	public String salary="";
	
	public Employee(String eid,String jobTitle,String salary){
		this.eid=eid;
		this.jobTitle=jobTitle;
		this.salary=salary;
	}
		
	public String getJobTitle() {
		return jobTitle;
	}

	public void setJobTitle(String jobTitle) {
		this.jobTitle = jobTitle;
	}

	public String getSalary() {
		return salary;
	}
	public void setSalary(String salary) {
		this.salary = salary;
	}
	public String getEid() {
		return eid;
	}
	public void setEid(String eid) {
		this.eid = eid;
	}
	
	
}
