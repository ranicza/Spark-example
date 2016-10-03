package com.epam.bigdata.q3.task8;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
import scala.Tuple2;

import java.lang.Iterable;

public class SparkUniqueWords {
	private static final Pattern SPACE = Pattern.compile(" ");

	  public static void main(String[] args) throws Exception {
		String inputFile = args[0];
	    String outputFile = args[1];
	    
	    if (args.length < 1) {
	      System.err.println("Usage: file <file>");
	      System.exit(1);
	    }

	    // Create a Java Spark Context.
	    SparkConf conf = new SparkConf().setAppName("uniqueWords");
			JavaSparkContext sc = new JavaSparkContext(conf);
		
		 // Load our input data.
		 JavaRDD<String> input = sc.textFile(inputFile);
		 
		 //Get separate params from row
		 JavaRDD<String> params = input.flatMap(new FlatMapFunction<String, String>() {
		      @Override
		      public Iterator<String> call(String s) {
		    	  System.out.println(Arrays.asList(SPACE.split(s)).iterator());
		        return Arrays.asList(SPACE.split(s)).iterator();
		      }
		   });
		 
		    // Transform into word and count.
		    JavaPairRDD<String, Integer> counts = params.mapToPair(
		      new PairFunction<String, String, Integer>(){
		        public Tuple2<String, Integer> call(String x){
		          return new Tuple2(x, 1);
		        }}).reduceByKey(new Function2<Integer, Integer, Integer>(){
		            public Integer call(Integer x, Integer y){ return x + y;}});
		    // Save the word count back out to a text file, causing evaluation.
		    counts.saveAsTextFile(outputFile);	
	  }
}
