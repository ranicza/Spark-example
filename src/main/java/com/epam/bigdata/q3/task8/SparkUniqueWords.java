package com.epam.bigdata.q3.task8;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.text.SimpleDateFormat;
import org.apache.spark.sql.Encoder;
import scala.Tuple2;
import scala.Tuple3;
import org.apache.spark.sql.Row;

public class SparkUniqueWords {
	private static final String SPLIT = "\\s+";
	private static final String FORMAT = "yyyyMMdd";
	
	public static void main(String[] args) throws Exception {
		String logFile = args[0];
	    String tagFile = args[1];
	    String cityFile = args[2];

	    if (args.length < 2) {
	      System.err.println("Usage: file <file_input>  <file_output>");
	      System.exit(1);
	    }    
	    
	    SparkSession spark = SparkSession
	    	      .builder()
	    	      .appName("SparkUniqueWords")
	    	      .getOrCreate();

        //GET TAG_ID + LIST OF TAGS	    
        Dataset<String> data = spark.read().textFile(tagFile);
        String header = data.first();
        JavaRDD<String> tagsRdd =data.filter(x -> !x.equals(header)).javaRDD();

       // tagsRdd.filter(x -> !x.contains("ID"));
        JavaPairRDD<Long, List<String>> tagsPairs = tagsRdd.mapToPair(new PairFunction<String, Long, List<String>>() {
            public Tuple2<Long, List<String>> call(String line) {
                String[] parts = line.split(SPLIT);
                
                return new Tuple2<Long, List<String>>(Long.parseLong(parts[0]), Arrays.asList(parts[1].split(",")));
            }
        });
        Map<Long, List<String>> tagsMap = tagsPairs.collectAsMap();
        
      //GET CITIES
        JavaRDD<String> citiesRDD = spark.read().textFile(cityFile).javaRDD();
        JavaPairRDD<Integer, String> citiesIdsPairs = citiesRDD.mapToPair(new PairFunction<String, Integer, String>() {
            public Tuple2<Integer, String> call(String line) {
                String[] parts = line.split(SPLIT);
                return new Tuple2<Integer, String>(Integer.parseInt(parts[0]), parts[1]);
            }
        });
        Map<Integer, String> citiesMap = citiesIdsPairs.collectAsMap();
        
        
        JavaRDD<ULogEntity> logsRdd = spark.read().textFile(logFile).javaRDD().map(new Function<String, ULogEntity>() {
            @Override
            public ULogEntity call(String line) throws Exception {                
                String[] parts = line.split(SPLIT);
                
                List<String> tags = tagsMap.get(Long.parseLong(parts[parts.length-2]));
                String city = citiesMap.get(Long.parseLong(parts[parts.length-15]));
                String date = parts[1].substring(0,8);             
                return new ULogEntity(Long.parseLong(parts[parts.length-2]), Long.parseLong(parts[parts.length-15]), date, tags, city);
            }
        });

//        Encoder<ULogEntity> encoder = Encoders.bean(ULogEntity.class);
//        Dataset<ULogEntity> df = spark.createDataset(logsRdd.rdd(), encoder);
        
        
        Dataset<Row> df = spark.createDataFrame(logsRdd, ULogEntity.class);
        df.createOrReplaceTempView("logs");
        df.show();
        df.limit(15).show();

        
        
        spark.stop();
	  }

}
