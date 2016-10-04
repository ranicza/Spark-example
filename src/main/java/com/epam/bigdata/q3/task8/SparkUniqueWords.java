package com.epam.bigdata.q3.task8;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.util.ArrayList;
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
	    // skip header from the file
        Dataset<String> data = spark.read().textFile(tagFile);
        String header = data.first();
        JavaRDD<String> tagsRdd =data.filter(x -> !x.equals(header)).javaRDD();

        JavaPairRDD<Long, List<String>> tagsPairs = tagsRdd.mapToPair(new PairFunction<String, Long, List<String>>() {
            public Tuple2<Long, List<String>> call(String line) {
                String[] parts = line.split(SPLIT);             
                return new Tuple2<Long, List<String>>(Long.parseLong(parts[0]), Arrays.asList(parts[1].split(",")));
            }
        });
        Map<Long, List<String>> tagsMap = tagsPairs.collectAsMap();
        
      //GET CITIES
        // skip header from the file
        Dataset<String> cityData = spark.read().textFile(cityFile);
        String cityHeader = cityData.first();       
        JavaRDD<String> citiesRDD = cityData.filter(x -> !x.equals(cityHeader)).javaRDD();
        
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
        df.limit(50).show();


        JavaPairRDD<DateCity, List<String>> dateCityTags = logsRdd.mapToPair(new PairFunction<ULogEntity, DateCity, List<String>>() {
            @Override
            public Tuple2<DateCity, List<String>> call(ULogEntity entity) {
                DateCity dc = new DateCity(entity.getTimestampDate(), entity.getCity());
                return new Tuple2<DateCity, List<String>>(dc, entity.getTags());
            }
        });
        
        JavaPairRDD<DateCity, List<String>> dayCityTagsPairs = dateCityTags.reduceByKey(new Function2<List<String>, List<String>, List<String>>() {
            @Override
            public List<String> call(List<String> i1, List<String> i2) {
                List<String> a1 = new ArrayList<>(i1);
                List<String> a2 = new ArrayList<>(i2);

                a1.removeAll(a2);
                a1.addAll(a2);
                return a1;
            }
        });
        
        
        List<Tuple2<DateCity, List<String>>> output = dayCityTagsPairs.collect();
        System.out.println("UNIQUE KEYWORDS - DATE/CITY");
        for (Tuple2<DateCity, List<String>> tuple : output) {
            System.out.println("CITY : " + tuple._1().getCity() + " DATE: " + tuple._1().getDate() + " TAGS: ");
            for (String tag : tuple._2()) {
                System.out.print(tag + " ");
            }
        }
        
        spark.stop();
	  }

}
