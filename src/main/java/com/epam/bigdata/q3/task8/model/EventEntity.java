package com.epam.bigdata.q3.task8.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class EventEntity implements Serializable{

	private String description;
	
	private String id;
	
	private String city;
	
	private String startDate;
	
	private String name;
	
	private int attendingCount;

	private String tag;
	
    private Map<String, Integer> countedWords = new HashMap<String, Integer>();
	
	private List<String> wordsFromDescription = new ArrayList<String>(); 
	
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

	public List<String> getWordsFromDescription() {
		return wordsFromDescription;
	}

	public void setWordsFromDescription(List<String> wordsFromDescription) {
		this.wordsFromDescription = wordsFromDescription;
	}

	public Map<String, Integer> getCountedWords() {
		return countedWords;
	}

	public void setCountedWords(Map<String, Integer> countedWords) {
		this.countedWords = countedWords;
	}


}
