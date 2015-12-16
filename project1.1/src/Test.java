import java.lang.String;
import java.lang.System;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringEscapeUtils;

public class Test {
    public static void main(String[] args) throws ParseException {
//    	String a = "RT @baekdoyehet: \\u0E16\\u0E49\\u0E32\\u0E04\\u0E38\\u0E13\\u0E21\\u0E35\\u0E1F\\u0E04 100 \\u0E04\\u0E19\\u0E09\\u0E31\\u0E19\\u0E04\\u0E37\\u0E2D\\u0E2B\\u0E19\\u0E36\\u0E48\\u0E07\\u0E43\\u0E19\\u0E19\\u0E31\\u0E49\\u0E19\\n\\u0E16\\u0E49\\u0E32\\u0E04\\u0E38\\u0E13\\u0E21\\u0E35\\u0E1F\\…";
//    	String b = StringEscapeUtils.unescapeJava(a);
//        System.out.println(b);
//    	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//    	Date fromDate = sdf.parse("2008-2-12");
//    	Date toDate = sdf.parse("2008-3-12");
//    	System.out.println("From " + fromDate);
//    	System.out.println("To " + toDate);
//
//    	Calendar cal = Calendar.getInstance();
//    	cal.setTime(fromDate);
//    	while (cal.getTime().before(toDate)) {
//    	    cal.add(Calendar.DATE, 1);
//    	    String date = sdf.format(cal.getTime());
//    	    System.out.println(date);
//    	}
    	
    	// fakeTID,date,score,tweetid,text
	// String date, String score, String tweetId, String text
    	String startdate = "2014-04-01", enddate="2014-04-08", userid = "999989377", n="3";
		String result ="Positive Tweets\n";
		int retrievedNumber = Integer.parseInt(n);
//		int id = hashFunc(userid);
		
//			set = jedis1.zrange(userid, 0, -1);
//		} else {
//			set = jedis2.zrange(userid, 0, -1);
//		}
		try {
			ArrayList<String> response = new ArrayList<String>();
			response.add("550618330954842111,2014-03-28,1692,449381669045157888 he's currently talking about a drug deal wow");
			response.add("547066732760641535,2014-04-06,872,452933267239358464,RT @GolanNentry: Never let go of the ones you care for");
			response.add("547007299468771326,2014-04-07,-872,452992700531228673,if you just got out of a really long relationship &amp; get into another relationship just to make your ex jealous, I'm judging you");
			response.add("548422382153469951,2014-04-03,-1281,451577617846530048,when my alarm goes off in the morning I think I'll just shoot myself instead of getting out of");			
			
//			if (id == 0) {
			// Add positive tweets
			int posCount = 0;
			for (int i = 0; i < response.size() && posCount < retrievedNumber; i++) {
				String[] values = response.get(i).split(",", 4);
				String date = values[1];
				if (Integer.parseInt(values[2]) < 0) {
					break;
				}
				String tidText = values[3].replace("\\n", "\n").replace("\\t", "\t").replace("\\r", "\r")
						.replace("\\\\", "\\");
				if (date.compareTo(startdate) >= 1 && enddate.compareTo(date) >= 1) {
					posCount++;
					result += date + "," + values[2] + "," + tidText + "\n";
				}
			}

			// Add negative tweets
			result += "Negative Tweets\n";
			int negCount = 0;
			for (int i = response.size() - 1; i >= 0 && negCount < retrievedNumber; i--) {
				String[] values = response.get(i).split(",", 4);
				String date = values[1];
				if (Integer.parseInt(values[2]) > 0) {
					break;
				}
				String tidText = values[3].replace("\\n", "\n").replace("\\t", "\t").replace("\\r", "\r")
						.replace("\\\\", "\\");
				if (date.compareTo(startdate) >= 1 && enddate.compareTo(date) >= 1) {
					negCount++;
					result += date + "," + values[2] + "," + tidText + "\n";
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println(result);
	}
    
}