package com.epam.bigdata.q3.task8;

import java.io.Serializable;

public class DateCity implements Serializable{
	
    private String date;
    private String city;
    
    
	public DateCity() {
		super();
	}


	public DateCity(String date, String city) {
		super();
		this.date = date;
		this.city = city;
	}


	public String getDate() {
		return date;
	}


	public void setDate(String date) {
		this.date = date;
	}


	public String getCity() {
		return city;
	}


	public void setCity(String city) {
		this.city = city;
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((city == null) ? 0 : city.hashCode());
		result = prime * result + ((date == null) ? 0 : date.hashCode());
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
		DateCity other = (DateCity) obj;
		if (city == null) {
			if (other.city != null)
				return false;
		} else if (!city.equals(other.city))
			return false;
		if (date == null) {
			if (other.date != null)
				return false;
		} else if (!date.equals(other.date))
			return false;
		return true;
	}


	@Override
	public String toString() {
		return "DateCity [date=" + date + ", city=" + city + "]";
	}
    
    
}
