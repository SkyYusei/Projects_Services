import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

import redis.clients.jedis.Jedis;


public class RedisParseQ4 {
	public static void main(String[] args) {
		Jedis jedis = new Jedis("localhost");
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			String line;
			while ((line = br.readLine()) != null) {
				String[] words = line.split("\t");
				String[] key = words[0].split(":");
				String hashtag = key[0];
				String date = key[1];
				String newValue = date + ":" + words[1];
				jedis.rpush(hashtag, newValue);
			}
			br.close();
			BufferedWriter bw = new BufferedWriter(new FileWriter("redis_success"));
			bw.write("success");
			bw.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}
