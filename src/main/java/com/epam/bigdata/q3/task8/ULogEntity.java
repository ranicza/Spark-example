package com.epam.bigdata.q3.task8;

import java.io.Serializable;
import java.util.Date;

public class ULogEntity implements Serializable{

	private long  IDUserTags;
	
	private long IDCity;
	
	private String timestampDate;

	public ULogEntity() {
	}

	public ULogEntity(long iDUserTags, long iDCity, String timestampDate) {
		super();
		IDUserTags = iDUserTags;
		IDCity = iDCity;
		this.timestampDate = timestampDate;
	}

	public long getIDUserTags() {
		return IDUserTags;
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

	@Override
	public String toString() {
		return "ULogEntity [IDUserTags=" + IDUserTags + ", IDCity=" + IDCity + ", timestampDate=" + timestampDate + "]";
	}



	
}
