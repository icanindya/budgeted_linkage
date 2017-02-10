package com.icanindya.linkage;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

public class Util {
	public static String getAge(String dob, String date){
		Date birthDate = new Date();
		Date currentDate = new Date();
		try {
			birthDate = new SimpleDateFormat("MM/dd/yyyy").parse(dob);
			currentDate = new SimpleDateFormat("MM/dd/yyyy").parse(date);
		} catch (ParseException e) {
			return "";
		}
		return  Integer.toString(getDiffYears(birthDate, currentDate)) ;
	}
	
	public static int getDiffYears(Date first, Date last) {
	    Calendar a = getCalendar(first);
	    Calendar b = getCalendar(last);
	    int diff = b.get(Calendar.YEAR) - a.get(Calendar.YEAR);
	    if (a.get(Calendar.MONTH) > b.get(Calendar.MONTH) || 
	        (a.get(Calendar.MONTH) == b.get(Calendar.MONTH) && a.get(Calendar.DATE) > b.get(Calendar.DATE))) {
	        diff--;
	    }
	    return diff;
	}

	public static Calendar getCalendar(Date date) {
	    Calendar cal = Calendar.getInstance(Locale.US);
	    cal.setTime(date);
	    return cal;
	}
	
	public static void main(String[] args){
		
	}
}