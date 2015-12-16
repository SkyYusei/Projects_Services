import java.util.ArrayList;

public class Test {
	public static void main(String[] args) {
		String tweets = "2014-04-23,-145,458813836661772288,pa enserio no te vallas[cmucchackers]2014-06-09,-180,475829532377612288,te escribí una carta y no me contestaste 888888888[cmucchackers]2014-04-26,-459,459888333422727168,@nataliab0294 muy bien le dire ajaja";
		String startDate = "2014-04-20";
		String endDate = "2015-02-18";
		int n = 7;
		String[] results = tweets.split("\\[cmucchackers\\]");
		ArrayList<String> positive = new ArrayList<String>();
		ArrayList<String> negative = new ArrayList<String>();
		int i = 0;
		int len = results.length;
		while(positive.size() < n) {
			if (i >= len || results[i].charAt(11) == '-') {
				break;
			}
			String currentDate = results[i].substring(0,10);
			if (currentDate.compareTo(startDate) >= 0 && currentDate.compareTo(endDate) <= 0) {
				positive.add(results[i]);
			}
			i++;
		}
		int j = 0;
		
		while(negative.size() < n) {
			if (j >= len || results[len - 1 - j].charAt(11) != '-') {
				break;
			}
			String currentDate = results[len - 1 - j].substring(0,10);
			if (currentDate.compareTo(startDate) >= 0 && currentDate.compareTo(endDate) <= 0) {
				negative.add(results[len-1-j]);
			}
			j++;
		}
		StringBuilder sb = new StringBuilder();
//		sb.append(TEAM_INFO);
		sb.append("Positive Tweets\n");
		for(String s : positive) {
			sb.append(s + "\n");
		}
		sb.append("\n");
		sb.append("Negative Tweets\n");
		for(String s : negative) {
			sb.append(s + "\n");
		}
		System.out.println(sb.toString());
	}
}
