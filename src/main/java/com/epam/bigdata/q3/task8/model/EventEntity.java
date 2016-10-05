package com.epam.bigdata.q3.task8.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class EventEntity implements Serializable{

	private String description;
	
	private String id;
	
	private String city;
	
	private String startDate;
	
	private String name;
	
	private int attendingCount;

	private String tag;
	
    private Map<String, Integer> words = new HashMap<String, Integer>();
	
	public EventEntity() {
		super();
	}

	public EventEntity(String description, String id, String name, int attendingSum, String tag) {
		super();
		this.description = description;
		this.id = id;
		this.name = name;
		this.attendingCount = attendingSum;
		this.tag = tag;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getStartDate() {
		return startDate;
	}

	public void setStartDate(String startDate) {
		this.startDate = startDate;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

	public int getAttendingCount() {
		return attendingCount;
	}

	public void setAttendingCount(int attendingCount) {
		this.attendingCount = attendingCount;
	}

	public Map<String, Integer> getWords() {
		return words;
	}

	public void setWords(Map<String, Integer> words) {
		this.words = words;
	}


	

	

}
