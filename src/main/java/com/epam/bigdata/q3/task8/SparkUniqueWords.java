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
	private static final String INPUT = "hdfs://sandbox.hortonworks.com:8020/tmp/admin/stream.20130607-ak.txt";
	private static final String OUTPUT = "hdfs://sandbox.hortonworks.com:8020/tmp/admin/out.txt";

	  public static void main(String[] args) throws Exception {
//		String inputFile = args[0];
//	    String outputFile = args[1];
		  
			String inputFile = INPUT;
		    String outputFile = OUTPUT;
		  
	    
	    if (args.length < 1) {
	      System.err.println("Usage: file <file_input>  <file_output>");
	      System.exit(1);
	    }

	    SparkConf conf = new SparkConf().setMaster("local[8]").setAppName("uniqueWords");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    JavaRDD<String> lines = sc.textFile(inputFile);
	    lines.cache();
	    
	    
	    
	    

		 //Get separate params from row
		 JavaRDD<String> params = lines.flatMap(new FlatMapFunction<String, String>() {
		      @Override
		      public Iterator<String> call(String s) {
		    	  System.out.println(Arrays.asList(SPACE.split(s)).iterator());
		        return Arrays.asList(SPACE.split(s)).iterator();
		      }
		   });
		 
//		 PairFunction<String, String, String> keyData= new PairFunction<String, String, String>() {
//			    @Override
//			    public Tuple2<String, String> call(String x) throws Exception {
//			        return new Tuple2(x.split(" ")[0], x.split("")[2]);
//			    }
//			};
			
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
