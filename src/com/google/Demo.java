package com.google;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

public class Demo {
	
   /* private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";*/

			  public static void main(String[] args) throws SQLException {
			   /*   try {
			      Class.forName(driverName);
			    } catch (ClassNotFoundException e) {
			      
			      e.printStackTrace();
			      System.exit(1);
			    }
			    
			    Connection con = DriverManager.getConnection("jdbc:hive2://10.10.0.30:10000/kiran", "hive", "");
			    Statement stmt = (Statement) con.createStatement();
			    String tableName = "kirantest";
			   String sql = "show tables '" + tableName + "'";
			    System.out.println("Running: " + sql);
			    ResultSet res = stmt.executeQuery(sql);
			    if (res.next()) {
			      System.out.println(res.getString(1));
			    }
			    
			    sql = "describe " + tableName;
			    System.out.println("Running: " + sql);
			    sql = "select * from " + tableName;
			    System.out.println("Running: " + sql);
			    res = stmt.executeQuery(sql);
			    while (res.next()) {
			      System.out.println(String.valueOf(res.getString(1)) + "\t" + res.getString(2)+ "\t" + res.getString(3)+ "\t" + res.getString(4)+ "\t" + res.getString(5)+ "\t" + res.getString(6)+ "\t" + res.getString(7)+ "\t" + res.getString(8)+ "\t" + res.getString(9));
			    }*/

				 SparkConf sparkConf = new SparkConf().setAppName("HiveConnector").setMaster("local[2]");
					    SparkContext sc = new SparkContext(sparkConf);   
				    HiveContext sqlContext = new org.apache.spark.sql.hive.HiveContext(sc);
				    String sqlQuery ="select * from kiran.kirantest"; 
				    DataFrame df = sqlContext.sql(sqlQuery);
				  df.show();
				}
		 }
		
	


