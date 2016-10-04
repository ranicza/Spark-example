package com.epam.bigdata.q3.task8;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.Function;


import java.util.Date;

import java.util.regex.Pattern;

import java.text.SimpleDateFormat;

public class SparkUniqueWords {
	private static final String SPLIT = "\\s+";
	private static final String FORMAT = "yyyyMMdd";
	
	
	//private static final Pattern SPACE = Pattern.compile(" ");
	private static final String INPUT = "hdfs://sandbox.hortonworks.com:8020/tmp/admin/stream.20130607-ak.txt";
	private static final String OUTPUT = "hdfs://sandbox.hortonworks.com:8020/tmp/admin/out1.txt";
	
	//private static final String TAB = "\\t";

	
	//private static final String INPUT = "fd46dc0f95eecaa17e223dab98268f57	20130607213331598	Vh1DCic73QFd3oL	Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; Sicent; WoShiHoney.B; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)	58.210.120.*	80	85	3	31drTvprdN1RFt	51bdec5216ef68820cfd3b2077a7a6b7	null	Edu_F_Upright	300	250	0	0	20	a499988a822facd86dd0e8e4ffef8532	300	1458	289291300775	0"+
//"ce58cba59ec01e66400c233d74c0ce4e	20130607213331602	VhK2CkN3PZF6Job	Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1	14.134.152.*	368	371	3	DD1SqS9rg5scFsf	46d439e5e082c02cf982adeaac8f07c	null	Auto_F_Rectangle	300	250	0	0	50	e1af08818a6cd6bbba118bb54a651961	254	3476	282825712806	1";
	 
	
	public static void main(String[] args) throws Exception {
//		String inputFile = args[0];
//	    String outputFile = args[1];

	    if (args.length < 1) {
	      System.err.println("Usage: file <file_input>  <file_output>");
	      System.exit(1);
	    }    
	    
	    SparkSession spark = SparkSession
	    	      .builder()
	    	      .appName("SparkUniqueWords")
	    	      .getOrCreate();

        JavaRDD<ULogEntity> logEntities = spark.read().text(INPUT).javaRDD().map(new Function<String, ULogEntity>() {
            @Override
            public ULogEntity call(String line) throws Exception {
                String[] parts = line.split(SPLIT);

                ULogEntity logEntity = new ULogEntity();
                logEntity.setIDUserTags(Integer.parseInt(parts[parts.length-2]));
                logEntity.setIDCity(Integer.parseInt(parts[parts.length-15]));

                SimpleDateFormat formatter = new SimpleDateFormat(FORMAT);
                String strDate = parts[1].substring(0,8);
                Date date = formatter.parse(strDate);
                logEntity.setTmsDate(date);
                return logEntity;
            }
        });
        
        logEntities.saveAsTextFile(OUTPUT);
        System.out.println();
	    
//        JavaRDD<ULogEntity> logEntities = spark.read().text(inputFile).javaRDD().map(new Function<String, ULogEntity>() {
//            @Override
//            public ULogEntity call(String line) throws Exception {
//                String[] parts = line.split(SPLIT);
//
//                ULogEntity logEntity = new ULogEntity();
//                logEntity.setIDUserTags(Integer.parseInt(parts[parts.length-2]));
//                logEntity.setIDCity(Integer.parseInt(parts[parts.length-15]));
//
//                SimpleDateFormat formatter = new SimpleDateFormat(FORMAT);
//                String strDate = parts[1].substring(0,8);
//                Date date = formatter.parse(strDate);
//                logEntity.setTmsDate(date);
//                return logEntity;
//            }
//        });
        
       
        

	    

//	    SparkConf conf = new SparkConf().setMaster("local[8]").setAppName("uniqueWords");
//	    SparkConf conf = new SparkConf().setAppName("uniqueWords");
//	    JavaSparkContext sc = new JavaSparkContext(conf);    
//	    
//	    JavaRDD<String> lines = sc.textFile(inputFile);
//	    lines.cache();
/*
		 //Get separate params from row
		 JavaRDD<String> params = lines.flatMap(new FlatMapFunction<String, String>() {
		      @Override
		      public Iterator<String> call(String s) {
		    	  System.out.println(Arrays.asList(TAB.split(s)).iterator());
		        return Arrays.asList(TAB.split(s)).iterator();
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
		    */
	  }


}
