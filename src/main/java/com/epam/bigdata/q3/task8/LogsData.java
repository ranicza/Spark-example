package com.epam.bigdata.q3.task8;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class LogsData {
	private static final String FORMAT = "yyyyMMdd";
	private static final String SPLIT = "\\s+";
	
	 public static List<ULogEntity> sampleData() throws IOException{
		 SimpleDateFormat formatter = new SimpleDateFormat(FORMAT);

	        return sampleDataAsStrings().stream().map(line -> {
	            String[] parts = line.split(SPLIT);
	            String strDate = parts[1].substring(0,8);
	            Date date= null;
					try {
						date = formatter.parse(strDate);
					} catch (java.text.ParseException e) {
						System.out.println(e.getMessage());
						e.printStackTrace();
					}
	            return new ULogEntity(Integer.parseInt(parts[parts.length-2]), Integer.parseInt(parts[parts.length-15]), date);
	        }).collect(Collectors.toList());
	    }

	    public static List<String> sampleDataAsStrings() throws IOException {
	        List ret = new ArrayList<String>();
	        BufferedReader r = new BufferedReader(new FileReader(new File("src/main/resources/sample_data.csv")));
	        String line;
	        while ((line=r.readLine())!=null) {
	            ret.add(line);
	        }
	        r.close();
	        return ret;
	    }
}
