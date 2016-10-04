package com.epam.bigdata.q3.task8;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

public class SparkUniqueWords {
	private static final String SPLIT = "\\s+";
	private static final String FORMAT = "yyyyMMdd";
	
	
	//private static final Pattern SPACE = Pattern.compile(" ");
	//private static final String INPUT = "hdfs://sandbox.hortonworks.com:8020/tmp/admin/stream.20130607-ak.txt";
	//private static final String OUTPUT = "hdfs://sandbox.hortonworks.com:8020/tmp/admin/out1.txt";
	
	//private static final String TAB = "\\t";

	
	//private static final String INPUT = "fd46dc0f95eecaa17e223dab98268f57	20130607213331598	Vh1DCic73QFd3oL	Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; Sicent; WoShiHoney.B; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)	58.210.120.*	80	85	3	31drTvprdN1RFt	51bdec5216ef68820cfd3b2077a7a6b7	null	Edu_F_Upright	300	250	0	0	20	a499988a822facd86dd0e8e4ffef8532	300	1458	289291300775	0"+
//"ce58cba59ec01e66400c233d74c0ce4e	20130607213331602	VhK2CkN3PZF6Job	Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1	14.134.152.*	368	371	3	DD1SqS9rg5scFsf	46d439e5e082c02cf982adeaac8f07c	null	Auto_F_Rectangle	300	250	0	0	50	e1af08818a6cd6bbba118bb54a651961	254	3476	282825712806	1";

	
	public static void main(String[] args) throws Exception {
		String inputFile = args[0];
	    String outputFile = args[1];

	    if (args.length < 1) {
	      System.err.println("Usage: file <file_input>  <file_output>");
	      System.exit(1);
	    }    
	    
	    SparkSession spark = SparkSession
	    	      .builder()
	    	      .appName("SparkUniqueWords")
	    	      .getOrCreate();

        JavaRDD<ULogEntity> logs = spark.read().text(inputFile).javaRDD().map(new Function<String, ULogEntity>() {
        	SimpleDateFormat formatter = new SimpleDateFormat(FORMAT);
            @Override
            public ULogEntity call(String line) throws Exception {
                String[] parts = line.split(SPLIT);
                String strDate = parts[1].substring(0,8);
                Date date = formatter.parse(strDate);
                return new ULogEntity(Integer.parseInt(parts[parts.length-2]), Integer.parseInt(parts[parts.length-15]), date);
            }
        });
        
        Encoder<ULogEntity> encoder = Encoders.bean(ULogEntity.class);

        Dataset<ULogEntity> df = spark.createDataset(logs.rdd(), encoder);

        df.limit(10).show();
        

	  }

	
	/* 
    public static List<ULogEntity> sampleData(String path) throws IOException{
		 SimpleDateFormat formatter = new SimpleDateFormat(FORMAT);

	        return sampleDataAsStrings().stream().map(line -> {
	            String[] parts = line.split(SPLIT);
	            String strDate = parts[1].substring(0,8);
	            Date date= null;
					try {
						date = formatter.parse(strDate);
					} catch (java.text.ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	            return new ULogEntity(Integer.parseInt(parts[parts.length-2]), Integer.parseInt(parts[parts.length-15]), date);
	        }).collect(Collectors.toList());
	    }

//	    public static List<String> sampleDataAsStrings() throws IOException {
//	        List ret = new ArrayList<String>();
//	        BufferedReader r = new BufferedReader(new FileReader(new File("src/main/resources/sample_data.csv")));
//	        String line;
//	        while ((line=r.readLine())!=null) {
//	            ret.add(line);
//	        }
//	        r.close();
//	        return ret;
//	    }
	    */
	

}
