package com.epam.bigdata.q3.task8;

import java.io.Serializable;
import java.util.Date;

public class ULogEntity implements Serializable{

	private long  IDUserTags;
	
	private long IDCity;
	
	private Date tmsDate;

	public ULogEntity() {
	}


	public ULogEntity(long iDUserTags, long iDCity, Date tmsDate) {
		super();
		IDUserTags = iDUserTags;
		IDCity = iDCity;
		this.tmsDate = tmsDate;
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

	public Date getTmsDate() {
		return tmsDate;
	}

	public void setTmsDate(Date tmsDate) {
		this.tmsDate = tmsDate;
	}

	@Override
	public String toString() {
		return "ULogEntity [IDUserTags=" + IDUserTags + ", IDCity=" + IDCity + ", tmsDate=" + tmsDate + "]";
	}

	
}
