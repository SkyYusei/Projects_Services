import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.InputStreamReader;

import redis.clients.jedis.Jedis;

public class SSDBParseQ3 {
	public static void main(String[] args) {
		Jedis jedis1 = new Jedis("54.173.248.225", 6379, 99999);
		Jedis jedis2 = new Jedis("52.91.235.18", 6379, 99999);
		Jedis jedis3 = new Jedis("54.210.7.64", 6379, 99999);
		Jedis jedis4 = new Jedis("54.210.13.85", 6379, 99999);    
		try {

			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			String line;
			while ((line = br.readLine()) != null) {
				String[] words = line.split("\t");
				String[] key = words[0].split(":");
				String[] values = words[1].split("\\[cmucchackers\\]");

				String userId = key[0];
				String date = key[1];

				for (String value : values) {
					String[] temp = value.split(",", 3);
					String score = temp[0];
					String tweetId = temp[1];
					String text = temp[2];
					int scoreInt =  Integer.parseInt(score);
					StringBuilder fakeTId = new StringBuilder();
					for (char c : tweetId.toCharArray()) {
						fakeTId.append((char)('9' - c + '0'));
					}
					String newValue = fakeTId.toString() + "," + date + "," + score + "," + tweetId + "," + text;

					int hashCode = hashFunc(userId);
					if (hashCode == 0)
						jedis1.zadd(userId, (double)scoreInt, newValue);
					else if (hashCode == 1)
						jedis2.zadd(userId, (double)scoreInt, newValue);
					else if (hashCode == 2)
						jedis3.zadd(userId, (double)scoreInt, newValue);
					else
						jedis4.zadd(userId, (double)scoreInt, newValue);
				}
			}
			br.close();
			BufferedWriter bw = new BufferedWriter(new FileWriter("ssdb_success"));
			bw.write("success");
			bw.close();
			jedis1.close();
			jedis2.close();
			jedis3.close();
			jedis4.close();
		} catch (Exception e) {
			jedis1.close();
			jedis2.close();
			jedis3.close();
			jedis4.close();
			e.printStackTrace();
		}

	}

	private static int hashFunc(String userId) {
		long result = userId.hashCode();
		return (int)(Math.abs(result) % 4);
	}
}