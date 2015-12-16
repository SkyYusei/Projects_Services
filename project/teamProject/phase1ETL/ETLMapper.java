import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TimeZone;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class ETLMapper {

	public static HashMap<String, Integer> sentimentMap() throws Exception {
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		BufferedReader br = new BufferedReader(new FileReader(new File(
				"afinn.txt")));
		String s;
		while ((s = br.readLine()) != null) {
			String[] line = s.split("\t");
			map.put(line[0], Integer.parseInt(line[1]));
		}
		br.close();
		return map;
	}

	public static HashSet<String> censorSet() throws Exception {
		HashSet<String> hs = new HashSet<String>();
		BufferedReader br = new BufferedReader(new FileReader(new File(
				"banned.txt")));
		String s;
		while ((s = br.readLine()) != null) {
			String term = s.trim();
			char[] transformed = new char[term.length()];
			for (int i = 0; i < term.length(); i++) {

				char c = term.charAt(i);
				if (Character.isLetter(c)) {
					c = (char) (c > 109 ? c - 13 : c + 13);
				}
				transformed[i] = c;
			}
			hs.add(String.valueOf(transformed));
		}
		br.close();
		return hs;
	}

	public static String[] parseLine(String s, HashMap<String, Integer> map,
			HashSet<String> set) throws Exception {

		SimpleDateFormat ft = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy");
		ft.setTimeZone(TimeZone.getTimeZone("+0000"));
		Date stampDate = ft.parse("Sun Apr 20 00:00:00 +0000 2014");
		
		SimpleDateFormat ftOut = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		ftOut.setTimeZone(TimeZone.getTimeZone("+0000"));
		String[] res = new String[2];
		try {
			JsonObject json = (JsonObject) new JsonParser().parse(s);
			String date = json.get("created_at").getAsString();
			Date createDate = ft.parse(date);
			if (createDate.after(stampDate)) {
				JsonObject temp = json.getAsJsonObject("user");
				String user_id = temp.get("id").getAsString();
				res[0] = user_id;
				StringBuilder sb = new StringBuilder();
				sb.append(ftOut.format(createDate).toString());
				sb.append("\t");
				String id = json.get("id").getAsString();
				sb.append(id);
				sb.append("\t");
				String text = json.get("text").getAsString();
				int score = 0;
				char[] textLine = text.toCharArray();
				int start = -1;
				int count = 0;
				for (int i = 0; i < textLine.length; i++) {
					if (Character.isLetterOrDigit(textLine[i])) {
						if (start == -1) {
							start = i;
						}
						count++;
					} else {
						if (start != -1) {
							String token = String.valueOf(textLine, start,
									count);
							token = token.toLowerCase();
							if (map.containsKey(token)) {
								score += map.get(token);
							}
							if (set.contains(token)) {
								for (int j = start + 1; j < start + count - 1; j++) {
									textLine[j] = '*';
								}
							}
							start = -1;
							count = 0;
						}
					}
				}
				if (start != -1) {
					String token = String.valueOf(textLine, start, count);
					token = token.toLowerCase();
					if (map.containsKey(token)) {
						score += map.get(token);
					}
					if (set.contains(token)) {
						for (int j = start + 1; j < start + count - 1; j++) {
							textLine[j] = '*';
						}
					}
				}
				sb.append(score);
				sb.append("\t");
				String censored_text = String.valueOf(textLine)
						.replace("\\", "\\\\").replace("\r", "\\r")
						.replace("\n", "\\n").replace("\t", "\\t");
				sb.append(censored_text);
				res[1] = sb.toString();
				return res;
			} else {
				return null;
			}
		} catch (Exception e) {
			return null;
		}
	}

	public static void main(String[] args) throws Exception {
		HashMap<String, Integer> map = sentimentMap();
		HashSet<String> set = censorSet();

		BufferedReader br = new BufferedReader(new InputStreamReader(System.in ));
		String s;
		while ((s = br.readLine()) != null) {
			String[] line = parseLine(s, map, set);
			if (line != null) {
				System.out.println(line[0] + "\t" + line[1]);
			}
		}
		br.close();
	}
}
