package com.epam.bigdata.q3.task8;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

public class ULogEntity implements Serializable{

	private long  IDUserTags;
	
	private long IDCity;
	
	private String timestampDate;
	
	private List<String> tags;
	
    private String city;

	public ULogEntity() {
	}

//	public ULogEntity(long iDUserTags, long iDCity, String timestampDate) {
//		super();
//		IDUserTags = iDUserTags;
//		IDCity = iDCity;
//		this.timestampDate = timestampDate;
//	}

	
	
	public long getIDUserTags() {
		return IDUserTags;
	}

	public ULogEntity(long iDUserTags, long iDCity, String timestampDate, List<String> tags, String city) {
		super();
		IDUserTags = iDUserTags;
		IDCity = iDCity;
		this.timestampDate = timestampDate;
		this.tags = tags;
		this.city = city;
	}

	public void setIDUserTags(long iDUserTags) {
		IDUserTags = iDUserTags;
	}

	public long getIDCity() {
		return IDCity;
	}

	public void setIDCity(long iDCity) {
		IDCity = iDCity;
	}

	public String getTimestampDate() {
		return timestampDate;
	}

	public void setTimestampDate(String timestampDate) {
		this.timestampDate = timestampDate;
	}

	public List<String> getTags() {
		return tags;
	}

	public void setTags(List<String> tags) {
		this.tags = tags;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	@Override
	public String toString() {
		return "ULogEntity [IDUserTags=" + IDUserTags + ", IDCity=" + IDCity + ", timestampDate=" + timestampDate + "]";
	}



	
}
