package cc.cmu.edu.minisite;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import com.google.gson.*;

public class PostParser {
	public static void main(String[] args) {
		try {
			BufferedReader bf = new BufferedReader(new FileReader("posts.json"));
			BufferedWriter bw = new BufferedWriter(new FileWriter("out"));
			String line;
			while ((line = bf.readLine()) != null) {
				JsonObject post = (JsonObject)new JsonParser().parse(line);
				String uid = post.get("uid").getAsString();
				String timestamp = post.get("timestamp").getAsString();
				
				StringBuilder sb = new StringBuilder();
				sb.append("UserID");
				sb.append((char)3);
				sb.append("{\"n\":\"" + uid + "\"}");
				sb.append((char)2);
				sb.append("Timestamp");
				sb.append((char)3);
				sb.append("{\"s\":\"" + timestamp + "\"}");
				sb.append((char)2);
				sb.append("Post");
				sb.append((char)3);
				sb.append("{\"s\":\"" + line.replace("\"", "\\\"") + "\"}");
				sb.append("\n");
				
				bw.write(sb.toString());
			}
			bf.close();
			bw.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}
}
