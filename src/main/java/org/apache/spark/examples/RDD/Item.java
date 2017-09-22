package org.apache.spark.examples.RDD;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class Item implements Serializable{
    private String ItemName;
    private String ItemNo;
    private String ItemSource;
    private Integer Total;
    public Item(){
    	
    }
    public Item(String name,String no,String source,int total){
    	this.ItemName=name;
    	this.ItemNo=no;
    	this.ItemSource=source;
    	this.Total=total;
    }
    public String getItemName(){
        return ItemName;
    }
 
    public String getItemNo(){
        return ItemNo;
    }

    public String getItemSource(){
        return ItemSource;
    }
 
    public Integer getTotal(){
        return Total;
    }
	public void setItemName(String itemName) {
		ItemName = itemName;
	}
	public void setItemNo(String itemNo) {
		ItemNo = itemNo;
	}
	public void setItemSource(String itemSource) {
		ItemSource = itemSource;
	}
	public void setTotal(Integer total) {
		Total = total;
	}
    
}