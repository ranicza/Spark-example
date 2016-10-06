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
import com.restfb.types.Location;
import com.restfb.types.Place;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
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
	private static final String TOKEN = "EAACEdEose0cBAOrGDDOmVisKgwTZA0rpAs9djoYHpBo9wzpteoEJ4yHGYZBWkZA1l5TlHtuNyYSUbzKS8WpkMSeEBjI7RZCytGqcHq5PmcYN8x8rApXYwskzAB30RIPu0QUZBagVdTJ3bRpdsopyGOwkF8GUeMFJbho3GHUMZAHCXiLYnARaLp";
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
        JavaPairRDD<DateCityEntity, Set<String>> dateCityTags = logsRdd.mapToPair(new PairFunction<ULogEntity, DateCityEntity, Set<String>>() {
            @Override
            public Tuple2<DateCityEntity, Set<String>> call(ULogEntity entity) {
                DateCityEntity dc = new DateCityEntity(entity.getTimestampDate(), entity.getCity());
                return new Tuple2<DateCityEntity, Set<String>>(dc, new HashSet<String>(entity.getTags()));
            }
        });

        /*
         * 	reduceByKey(Function2<V,V,V> func)
		 *	Merge the values for each key using an associative reduce function.
		 *  This will also perform the merging locally on each mapper before sending
		 *  results to a reducer, similarly to a "combiner" in MapReduce.
		 *  Output will be hash-partitioned with the existing partitioner/ parallelism level.
         */
        JavaPairRDD<DateCityEntity, Set<String>> dateCityTagsPairs = dateCityTags.reduceByKey(new Function2<Set<String>, Set<String>, Set<String>>() {
            @Override
            public Set<String> call(Set<String> set1, Set<String> set2) {
                set1.addAll(set2);
                return set1;
            }
        });

        
        System.out.println("-----------------------UNIQUE KEYWORDS PER DATE/CITY----------------------------");
        for (Tuple2<DateCityEntity, Set<String>> tuple : dateCityTagsPairs.collect()) {
            System.out.println("CITY : " + tuple._1().getCity() + " DATE: " + tuple._1().getDate() + " TAGS: ");
            if (tuple._2 != null) {
                for (String tag : tuple._2()) {
                    System.out.print(tag + " ");
                }
            }
        }

      //---------------------------------FACEBOOK API PART---------------------------------------------------
        
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
        JavaRDD<TagEventsEntity> tagsEventsRDD = uniqueTagsRdd.map(new Function<String, TagEventsEntity>() {
			@Override
			public TagEventsEntity call(String tag) throws Exception {
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
	                            System.out.println(event.getDescription());
	                            if (!event.getDescription().isEmpty() || event.getDescription()== null) {
	                                String[] words = event.getDescription().split(SPLIT);

	                                for (String word : words) {
	                                    Integer n = eventEntity.getWords().get(word);
	                                    if (n == null) {
	                                        eventEntity.getWords().put(word, 1);
	                                    } else {
	                                        eventEntity.getWords().put(word, n + 1);
	                                    }
	                                }
	                            }
	                            
	                            
	                            
	                            /*
	                            if (StringUtils.isNotEmpty(event.getDescription()) && StringUtils.isNotBlank(event.getDescription())) {
	                                String[] words = event.getDescription().split(SPLIT);

	                                for (String word : words) {
	                                    Integer n = eventEntity.getWords().get(word);
	                                    if (n == null) {
	                                        eventEntity.getWords().put(word, 1);
	                                    } else {
	                                        eventEntity.getWords().put(word, n + 1);
	                                    }
	                                }
	                            }
	                            */
	                            eventsByTag.add(eventEntity);							 
						 }
					 }
				}
                TagEventsEntity tagEvents = new TagEventsEntity(tag, eventsByTag);
                System.out.println("TAG: " + tag +  " , amount of events: " + eventsByTag.size());
				return tagEvents;
			}      	
		});
        
        
        JavaRDD<EventEntity> allEventsRdd = tagsEventsRDD.flatMap(tagEvents ->
        	tagEvents.getEvents().iterator()
        );

        JavaPairRDD<DateCityTagEntity, EventEntity> dateCityTagsByPairs = allEventsRdd.mapToPair(eventEntity -> {
        	DateCityTagEntity dctEntity = new DateCityTagEntity(eventEntity.getStartDate(), eventEntity.getCity(), eventEntity.getTag());
        	 return new Tuple2<DateCityTagEntity, EventEntity>(dctEntity, eventEntity);
        	}
        );
        
        /*
         * reduceByKey(Function2<V,V,V> func)
         */        
        JavaPairRDD<DateCityTagEntity, EventEntity> dctPairs2 = dateCityTagsByPairs.reduceByKey((event1, event2) -> {
        	EventEntity eventEntity = new EventEntity();
        	eventEntity.setAttendingCount(event1.getAttendingCount() + event2.getAttendingCount());
			/*
			Map<String, Integer> words1 = event1.getWords();
			Map<String, Integer> words2 = event2.getWords();
			for (String word : words2.keySet()) {
				Integer num1 = words1.get(word);
				Integer num2 = words2.get(word);			
				//num1==null ? words1.put(word, num2) : words1.put(word, num1 + num2);				
				if (num1 == null ) {
					words1.put(word, num2);
				} else {
					 words1.put(word, num1 + num2);
				}
			}
			eventEntity.setWords(words1);    
			*/  	
        	return eventEntity;
        });
        
        
        System.out.println("--------------------KEYWORD DAY CITY TOTAL_AMOUNT_OF_VISITORS TOKEN_MAP(KEYWORD_1, AMOUNT_1... KEYWORD_N, AMOUNT_N)-------------------");
        
        dctPairs2.collect().forEach(tuple -> {
        	System.out.println("KEYWORD: " + tuple._1().getTag() + " DATE: " + tuple._1().getDate() + " CITY: " + tuple._1().getCity());
        	System.out.println("TOTAL_AMOUNT_OF_VISITORS : " + tuple._2.getAttendingCount());
//        	Map<String, Integer> sortedMap = tuple._2.getWords().entrySet().stream()
//                    .sorted(Map.Entry.comparingByValue(java.util.Comparator.reverseOrder())).limit(10)
//                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
//
//            System.out.println("TOKEN_MAP(KEYWORD_1, AMOUNT_1... KEYWORD_10, AMOUNT_10)  : ");
//            for (String str : sortedMap.keySet()) {
//                System.out.print(str + " " + sortedMap.get(str) + " | ");
//            }
        });
        
        

        spark.stop();
	  }

}
