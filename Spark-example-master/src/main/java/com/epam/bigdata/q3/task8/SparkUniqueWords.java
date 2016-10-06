package com.epam.bigdata.q3.task8;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import com.epam.bigdata.q3.task8.model.DateCityEntity;
import com.epam.bigdata.q3.task8.model.DateCityTagEntity;
import com.epam.bigdata.q3.task8.model.EventEntity;
import com.epam.bigdata.q3.task8.model.TagEventsEntity;
import com.epam.bigdata.q3.task8.model.ULogEntity;
import com.restfb.Connection;
import com.restfb.DefaultFacebookClient;
import com.restfb.FacebookClient;
import com.restfb.Parameter;
import com.restfb.Version;
import com.restfb.types.Event;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import scala.Tuple2;
import org.apache.spark.sql.Row;

public class SparkUniqueWords {
	private static final String SPLIT = "\\s+";
	private static final SimpleDateFormat dateFormatter = new SimpleDateFormat("mm.dd.yyyy");
	
	private static final String TOKEN = "EAACEdEose0cBAAwZBg0G60m6Wb9KtC45uwePtbKWK97aDE7BmUxEs0WZCNKe4dIHOShc7mssretzCwLoWwQPYZBOYa8DtmcfLdm5QDw6xf05mbOO8MpPlbn8T7H4VJ8JAocKHIJRDrn6AWoUB7Gm0VtW8BZC2kOfDt1rL9cO2V4Itq80nlsG";
	private static final FacebookClient facebookClient = new DefaultFacebookClient(TOKEN, Version.VERSION_2_5);
    
    
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
	    
	    //================ 	1. Collect all unique keyword per day per location (city)   ===========================

	    /*
	     * -------------------- GET TAG_ID + LIST OF TAGS -------------------------
	     */
        Dataset<String> data = spark.read().textFile(tagFile);
        String header = data.first();
        
	    // Reads file as a collection of lines skipping header from the file.
        JavaRDD<String> tagsRdd = data.filter(x -> !x.equals(header)).javaRDD();
        
        /*
         *  In Java, key-value pairs are represented using the scala.Tuple2 class
         *  from the Scala standard library. 
         *  RDDs of key-value pairs are represented by the JavaPairRDD class. 
         *  mapToPair(PairFunction<T,K2,V2> f) 
         */
        JavaPairRDD<Long, List<String>> tagsPairs = tagsRdd.mapToPair(line -> {
                String[] parts = line.split(SPLIT);             
                return new Tuple2<Long, List<String>>(Long.parseLong(parts[0]), Arrays.asList(parts[1].split(",")));
        });
        
        Map<Long, List<String>> tagsMap = tagsPairs.collectAsMap();
        
        
        /*
         * ----------------------- GET CITIES --------------------------------------
         */
        Dataset<String> cityData = spark.read().textFile(cityFile);
        String cityHeader = cityData.first();       
        JavaRDD<String> citiesRDD = cityData.filter(x -> !x.equals(cityHeader)).javaRDD();

        JavaPairRDD<Integer, String> citiesIdsPairs = citiesRDD.mapToPair(line -> {
                String[] parts = line.split(SPLIT);
                return new Tuple2<Integer, String>(Integer.parseInt(parts[0]), parts[1]);
        });
        
        Map<Integer, String> citiesMap = citiesIdsPairs.collectAsMap();
        
        /*
         * ----------------------- GET LOGS + TAGS + CITIES + DATE ------------------------
         * map(Function<T,R> f) 
         */
        JavaRDD<ULogEntity> logsRdd = spark.read().textFile(logFile).javaRDD().map(line -> {              
                String[] parts = line.split(SPLIT);             
                List<String> tags = tagsMap.get(Long.parseLong(parts[parts.length-2]));
                String city = citiesMap.get(Long.parseLong(parts[parts.length-15]));
                String date = parts[1].substring(0,8);             
                return new ULogEntity(Long.parseLong(parts[parts.length-2]), Long.parseLong(parts[parts.length-15]), date, tags, city);
        });

        Dataset<Row> df = spark.createDataFrame(logsRdd, ULogEntity.class);
        df.createOrReplaceTempView("logs");
        df.limit(15).show();


        /*
         *  Date/City by pairs
         *  RDDs of key-value pairs are represented by the JavaPairRDD class. 
         *  mapToPair(PairFunction<T,K2,V2> f) 
         */
        JavaPairRDD<DateCityEntity, Set<String>> dateCityTags = logsRdd.mapToPair(log-> {
       	 DateCityEntity dc = new DateCityEntity(log.getTimestampDate(), log.getCity());
            return new Tuple2<DateCityEntity, Set<String>>(dc, new HashSet<String>(log.getTags()));
       });

        /*
         * 	reduceByKey(Function2<V,V,V> func)
		 *	Merge the values for each key using an associative reduce function.
		 *  This will also perform the merging locally on each mapper before sending
		 *  results to a reducer, similarly to a "combiner" in MapReduce.
		 *  Output will be hash-partitioned with the existing partitioner/ parallelism level.
         */
        JavaPairRDD<DateCityEntity, Set<String>> dateCityTagsPairs = dateCityTags.reduceByKey((set1, set2) -> {
                set1.addAll(set2);
                return set1;
        });
        
//        JavaPairRDD<DateCityEntity, Set<String>> dateCityTagsPairs = dateCityTags.reduceByKey(new Function2<Set<String>, Set<String>, Set<String>>() {
//            @Override
//            public Set<String> call(Set<String> set1, Set<String> set2) {
//                set1.addAll(set2);
//                return set1;
//            }
//        });

        
        System.out.println("-----------------------UNIQUE KEYWORDS PER DATE/CITY----------------------------");
        for (Tuple2<DateCityEntity, Set<String>> tuple : dateCityTagsPairs.collect()) {
            System.out.println("CITY: " + tuple._1().getCity() + " DATE: " + tuple._1().getDate() + " TAGS: ");
            if (tuple._2 != null) {
                for (String tag : tuple._2()) {
                    System.out.print(tag + " ");
                }
            }
        }

      //===========================    2.  FACEBOOK API PART  ========================================================
        
       /*
        *  Get a sequence of all unique tags from the file.
        *  flatMap(FlatMapFunction<T,U> f) 
        */       
       JavaRDD<String> uniqueTagsRdd = logsRdd.flatMap(log -> 
       		log.getTags().iterator()
       ).distinct();

               
      /*
       * Get events according tag.  
       * map(Function<T,R> f) 
       */
       JavaRDD<TagEventsEntity> tagsEventsRDD = uniqueTagsRdd.map(tag -> {
				Connection<Event> con = facebookClient.fetchConnection("search", Event.class, Parameter.with("q", tag), 
						Parameter.with("type", "event"), Parameter.with("fields", "id,attending_count,place,name,description,start_time"));
				
				List<EventEntity> eventsByTag = new ArrayList<EventEntity>();
				for (List<Event> events : con) {
					 for (Event event : events) {
						 if (event != null) {
							 EventEntity eventEntity = new EventEntity(event.getDescription(), event.getId(), event.getName(), event.getAttendingCount(), tag);
							 
							 if (event.getPlace() != null && event.getPlace().getLocation() != null && event.getPlace().getLocation().getCity() != null) {
								 eventEntity.setCity(event.getPlace().getLocation().getCity());
	                         } else {
	                        	 eventEntity.setCity("undefined");
	                         }
	                            if (event.getStartTime() != null) {
	                            	eventEntity.setStartDate(dateFormatter.format(event.getStartTime()).toString());
	                            } else {
	                            	eventEntity.setStartDate("2016-10-05");
	                            }
							 
	                            // Get words from event's description
	                            if (StringUtils.isNotBlank(event.getDescription()) && StringUtils.isNotEmpty(event.getDescription())) {
	                                String[] words = event.getDescription().split(SPLIT);	                                
	                                for (String word : words) {
	                                	eventEntity.getWordsFromDescription().add(word);
	                                }
	                            }
	                            eventsByTag.add(eventEntity);							 
						 }
					 }
				}
               TagEventsEntity tagEvents = new TagEventsEntity(tag, eventsByTag);
               System.out.println("TAG: " + tag +  " , amount of events: " + eventsByTag.size());
				return tagEvents;     	
		});
       
//        JavaRDD<TagEventsEntity> tagsEventsRDD = uniqueTagsRdd.map(new Function<String, TagEventsEntity>() {
//			@Override
//			public TagEventsEntity call(String tag) throws Exception {
//				Connection<Event> con = facebookClient.fetchConnection("search", Event.class, Parameter.with("q", tag), 
//						Parameter.with("type", "event"), Parameter.with("fields", "id,attending_count,place,name,description,start_time"));
//				
//				List<EventEntity> eventsByTag = new ArrayList<EventEntity>();
//				for (List<Event> events : con) {
//					 for (Event event : events) {
//						 if (event != null) {
//							 EventEntity eventEntity = new EventEntity(event.getDescription(), event.getId(), event.getName(), event.getAttendingCount(), tag);
//							 
//							 if (event.getPlace() != null && event.getPlace().getLocation() != null && event.getPlace().getLocation().getCity() != null) {
//								 eventEntity.setCity(event.getPlace().getLocation().getCity());
//	                         } else {
//	                        	 eventEntity.setCity("undefined");
//	                         }
//	                            if (event.getStartTime() != null) {
//	                            	eventEntity.setStartDate(dateFormatter.format(event.getStartTime()).toString());
//	                            } else {
//	                            	eventEntity.setStartDate("2016-10-05");
//	                            }
//							 
//	                            // Get words from event's description
//	                            if (StringUtils.isNotBlank(event.getDescription()) && StringUtils.isNotEmpty(event.getDescription())) {
//	                                String[] words = event.getDescription().split(SPLIT);	                                
//	                                for (String word : words) {
//	                                	eventEntity.getWordsFromDescription().add(word);
//	                                }
//	                            }
//	                            eventsByTag.add(eventEntity);							 
//						 }
//					 }
//				}
//                TagEventsEntity tagEvents = new TagEventsEntity(tag, eventsByTag);
//                System.out.println("TAG: " + tag +  " , amount of events: " + eventsByTag.size());
//				return tagEvents;
//			}      	
//		});
        
        
        JavaRDD<EventEntity> allEventsRdd = tagsEventsRDD.flatMap(tagEvents ->
        	tagEvents.getEvents().iterator()
        );

        JavaPairRDD<DateCityTagEntity, EventEntity> dateCityTagsByPairs = allEventsRdd.mapToPair(eventEntity -> {
        	DateCityTagEntity dctEntity = new DateCityTagEntity(eventEntity.getStartDate(), eventEntity.getCity(), eventEntity.getTag());
        	 return new Tuple2<DateCityTagEntity, EventEntity>(dctEntity, eventEntity);
        	}
        );
        
       
        JavaPairRDD<DateCityTagEntity, EventEntity> dctPairs = dateCityTagsByPairs.reduceByKey((event1, event2) -> {
        	EventEntity eventEntity = new EventEntity();        	
        	eventEntity.setAttendingCount(event1.getAttendingCount() + event2.getAttendingCount());

        	Map<String, Integer> map = getCountedWords(event1.getWordsFromDescription(), event2.getWordsFromDescription());
        	eventEntity.setCountedWords(map);	
        	return eventEntity;
        });

        System.out.println("--------------------KEYWORD DAY CITY TOTAL_AMOUNT_OF_VISITORS TOKEN_MAP(KEYWORD_1, AMOUNT_1... KEYWORD_N, AMOUNT_N)-------------------");
        
        dctPairs.collect().forEach(tuple -> {
        	System.out.println("KEYWORD: " + tuple._1().getTag() + " DATE: " + tuple._1().getDate() + " CITY: " + tuple._1().getCity());
        	System.out.println("TOTAL_AMOUNT_OF_VISITORS : " + tuple._2.getAttendingCount());
        	
        	Map<String, Integer> outputWords = tuple._2.getCountedWords().entrySet().stream()
                    .sorted(Map.Entry.comparingByValue(java.util.Comparator.reverseOrder())).limit(10)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

            System.out.println("TOP 10 KEYWORDS: ");
            for (String str : outputWords.keySet()) {
                System.out.print(str + "(" + outputWords.get(str) + ")  ");
            }
        });
        
        
//      Encoder<DateCityTagEntity> encoder = Encoders.bean(DateCityTagEntity.class);
//      Dataset<ULogEntity> df = spark.createDataset(logsRdd.rdd(), encoder);
//        
//        Dataset<Row> result = spark.createDataFrame(dctPairs, DateCityTagEntity.class,);
//        result.createOrReplaceTempView("result");
//        result.show();
//        result.limit(15).show();
        
        spark.stop();
	  }

	
	
	
	private static Map<String, Integer> getCountedWords(List<String> words, List<String> words2) {
		Map<String, Integer> occuranceWords = new HashMap<String, Integer>();
		words.addAll(words2);
		for (String word : words) {
    		int occurrences = Collections.frequency(words, word);
    		occuranceWords.put(word, occurrences);
		}
		return occuranceWords;
	}
}
