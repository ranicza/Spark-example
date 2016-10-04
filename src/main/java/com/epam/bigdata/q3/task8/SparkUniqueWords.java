package com.epam.bigdata.q3.task8;

import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.Function;

import java.util.Date;

import java.text.SimpleDateFormat;
import org.apache.spark.sql.Encoder;


public class SparkUniqueWords {
	private static final String SPLIT = "\\s+";
	private static final String FORMAT = "yyyyMMdd";
	
	public static void main(String[] args) throws Exception {
		String inputFile = args[0];
	   // String outputFile = args[1];

	    if (args.length < 1) {
	      System.err.println("Usage: file <file_input>  <file_output>");
	      System.exit(1);
	    }    
	    
	    SparkSession spark = SparkSession
	    	      .builder()
	    	      .appName("SparkUniqueWords")
	    	      .getOrCreate();

        JavaRDD<ULogEntity> logs = spark.read().textFile(inputFile).javaRDD().map(new Function<String, ULogEntity>() {
            @Override
            public ULogEntity call(String line) throws Exception {                
                String[] parts = line.split(SPLIT);
                String date = parts[1].substring(0,8);             
                return new ULogEntity(Long.parseLong(parts[parts.length-2]), Long.parseLong(parts[parts.length-15]), date);
            }
        });

        Encoder<ULogEntity> encoder = Encoders.bean(ULogEntity.class);

        Dataset<ULogEntity> df = spark.createDataset(logs.rdd(), encoder);

        df.limit(10).show();
        
        spark.stop();
	  }

}
