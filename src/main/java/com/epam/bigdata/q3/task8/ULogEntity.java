package com.epam.bigdata.q3.task8;

import java.io.Serializable;
import java.util.Date;

public class ULogEntity implements Serializable{

	private static final long serialVersionUID = 1L;

	private int  IDUserTags;
	
	private int IDCity;
	
	private Date tmsDate;

	public ULogEntity() {
	}

	public ULogEntity(int iDUserTags, int iDCity, Date tmsDate) {
		super();
		IDUserTags = iDUserTags;
		IDCity = iDCity;
		this.tmsDate = tmsDate;
	}

	public int getIDUserTags() {
		return IDUserTags;
	}

	public void setIDUserTags(int iDUserTags) {
		IDUserTags = iDUserTags;
	}

	public int getIDCity() {
		return IDCity;
	}

	public void setIDCity(int iDCity) {
		IDCity = iDCity;
	}

	public Date getTmsDate() {
		return tmsDate;
	}

	public void setTmsDate(Date tmsDate) {
		this.tmsDate = tmsDate;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + IDCity;
		result = prime * result + IDUserTags;
		result = prime * result + ((tmsDate == null) ? 0 : tmsDate.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ULogEntity other = (ULogEntity) obj;
		if (IDCity != other.IDCity)
			return false;
		if (IDUserTags != other.IDUserTags)
			return false;
		if (tmsDate == null) {
			if (other.tmsDate != null)
				return false;
		} else if (!tmsDate.equals(other.tmsDate))
			return false;
		return true;
	}

	
	
}
