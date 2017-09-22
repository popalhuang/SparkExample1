package org.apache.spark.examples.SparkSession;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Database;

public class SparkSessionExample1 {

	public static void main(String[] args) {
		//Spark Init
		SparkSession spark = SparkSession.builder().appName("SparkSession.Example1").enableHiveSupport().getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");
		
		//Hive MetaData相關
		System.out.println("Hive MetaStore Info");
		System.out.println("-------------------");
		System.out.println("List All Databases:");
		spark.catalog().listDatabases().show();
		Dataset<Database> databaseList = spark.catalog().listDatabases().sort("name");
		for(Database db :databaseList.collectAsList() ){
			System.out.println(db.name()+","+db.locationUri());
			spark.catalog().setCurrentDatabase(db.name());
			System.out.println("current database:"+spark.catalog().currentDatabase());
			spark.catalog().listTables().show();
		}
		
		
		
		
		
		
		
	}

}
