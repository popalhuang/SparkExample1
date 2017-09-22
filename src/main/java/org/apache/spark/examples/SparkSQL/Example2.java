package org.apache.spark.examples.SparkSQL;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Example2 {

	public static void main(String[] args) throws ParseException {
		
		// TODO Auto-generated method stub
		SparkSession spark = SparkSession.builder().appName("SparkSQL.Example2").enableHiveSupport().getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		
		List<Row> data2 = new ArrayList<Row>();		
		for(long i=0;i<10000;i++){
			Map<String, String> map = new HashMap<String, String>();
			int zip32id = (int)(Math.random()*61904+1);
			map.put("zip32id",Integer.toString(zip32id));			
			SimpleDateFormat sf = new SimpleDateFormat("yyyy/MM/dd");
			byte age = (byte)(Math.random()*98+1);
			int brondate = age*365+(int)(Math.random()*365+1);												
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.DAY_OF_MONTH, -brondate);
			String birthday = sf.format(cal.getTime());
			java.sql.Date sql_birthday = new java.sql.Date(sf.parse(birthday).getTime());
			int gender = (int)(Math.random()*2+1);
			boolean g =(gender ==1)?true:false;
			System.out.println("age:"+age+",brondate:"+brondate+",birthday:"+sql_birthday+",gender:"+gender);
			data2.add(RowFactory.create(i,"\"test_"+i+"\"",age,sql_birthday,(Math.random()*70000+22000),g,map));
		}
		
		//Create StructFields
		List<StructField> kinsfolk_structFields = new ArrayList<StructField>();
		
		List<StructField> structFields = new ArrayList<StructField>();
		
		structFields.add(DataTypes.createStructField("eid",DataTypes.LongType, true));
		structFields.add(DataTypes.createStructField("name",DataTypes.StringType, true));		
		structFields.add(DataTypes.createStructField("age",DataTypes.ByteType, true));
		structFields.add(DataTypes.createStructField("birthday",DataTypes.DateType, true));
		structFields.add(DataTypes.createStructField("salary",DataTypes.DoubleType, true));
		structFields.add(DataTypes.createStructField("gender",DataTypes.BooleanType, true));
		structFields.add(DataTypes.createStructField("address",DataTypes.createMapType(DataTypes.StringType,DataTypes.StringType), true));
		
		StructType schema = DataTypes.createStructType(structFields);
		
		
		Dataset<Row> ds = spark.createDataFrame(data2, schema);
		
		ds.write().mode(SaveMode.Overwrite).saveAsTable("default.employee");
		
		
		
		
		
		
	}

}
