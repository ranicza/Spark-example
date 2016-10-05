package com.epam.bigdata.q3.task8;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import com.epam.bigdata.q3.task8.model.DateCityEntity;
import com.epam.bigdata.q3.task8.model.ULogEntity;
import com.restfb.DefaultFacebookClient;
import com.restfb.FacebookClient;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import scala.Tuple2;
import org.apache.spark.sql.Row;

public class SparkUniqueWords {
	private static final String SPLIT = "\\s+";
	
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

	    /*
	     * --------------------GET TAG_ID + LIST OF TAGS-------------------------
	     */
        Dataset<String> data = spark.read().textFile(tagFile);
        String header = data.first();
        
	    //reads file as a collection of lines skipping header from the file.
        JavaRDD<String> tagsRdd =data.filter(x -> !x.equals(header)).javaRDD();
        /*
         *  In Java, key-value pairs are represented using the scala.Tuple2 class
         *  from the Scala standard library. 
         *  RDDs of key-value pairs are represented by the JavaPairRDD class. 
         *  mapToPair(PairFunction<T,K2,V2> f) 
         */
        JavaPairRDD<Long, List<String>> tagsPairs = tagsRdd.mapToPair(new PairFunction<String, Long, List<String>>() {
            public Tuple2<Long, List<String>> call(String line) {
                String[] parts = line.split(SPLIT);             
                return new Tuple2<Long, List<String>>(Long.parseLong(parts[0]), Arrays.asList(parts[1].split(",")));
            }
        });
        Map<Long, List<String>> tagsMap = tagsPairs.collectAsMap();
        
        
        /*
         * -----------------------GET CITIES--------------------------------------
         */
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
        
        /*
         * -----------------------GET LOGS + TAGS + CITIES----------------------------
         * map(Function<T,R> f) 
         */
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


        /*
         *  Date/City pre pairs
         *  RDDs of key-value pairs are represented by the JavaPairRDD class. 
         *  mapToPair(PairFunction<T,K2,V2> f) 
         */
        JavaPairRDD<DateCityEntity, List<String>> dateCityTags = logsRdd.mapToPair(new PairFunction<ULogEntity, DateCityEntity, List<String>>() {
            @Override
            public Tuple2<DateCityEntity, List<String>> call(ULogEntity entity) {
                DateCityEntity dc = new DateCityEntity(entity.getTimestampDate(), entity.getCity());
                return new Tuple2<DateCityEntity, List<String>>(dc, entity.getTags());
            }
        });

        /*
         * 	reduceByKey(Function2<V,V,V> func)
		 *	Merge the values for each key using an associative reduce function.
		 *  This will also perform the merging locally on each mapper before sending
		 *  results to a reducer, similarly to a "combiner" in MapReduce.
		 *  Output will be hash-partitioned with the existing partitioner/ parallelism level.
         */
        JavaPairRDD<DateCityEntity, List<String>> dateCityTagsPairs = dateCityTags.reduceByKey(new Function2<List<String>, List<String>, List<String>>() {       	
            @Override
            public List<String> call(List<String> i1, List<String> i2) {
            	List<String> a1 = null;
            	if (i1 != null) {
            		 a1 = new ArrayList<>(i1);
            	}
                if (i2 != null) {
                    List<String> a2 = new ArrayList<>(i2);               
                    a1.removeAll(a2);
                    a1.addAll(a2);
                }

                return a1;
            }
        });
        
        
        List<Tuple2<DateCityEntity, List<String>>> result = dateCityTagsPairs.collect();
        System.out.println("-----------------------UNIQUE KEYWORDS PER DATE/CITY----------------------------");
        for (Tuple2<DateCityEntity, List<String>> tuple : result) {
            System.out.println("CITY : " + tuple._1().getCity() + " DATE: " + tuple._1().getDate() + " TAGS: ");
            if (tuple._2 != null) {
                for (String tag : tuple._2()) {
                    System.out.print(tag + " ");
                }
            }
        }
        
        
      //----------------------------FACEBOOK API PART-----------------------------------------
        
       //FacebookClient facebookClient = new DefaultFacebookClient("MY_ACCESS_TOKEN");
        
        
        
        
        
        
        
        
        
        
        
        
        spark.stop();
	  }

}
