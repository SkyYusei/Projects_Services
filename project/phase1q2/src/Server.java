import io.vertx.core.AbstractVerticle;
import io.vertx.core.MultiMap;

import java.util.HashMap;
import java.util.List;

public class Server extends AbstractVerticle {
	// private Decipher decipher = new Decipher();
	// Convenience method so you can run it in your IDE

	private static EntryDAO entryDAO = new EntryDAO();
	private static HashMap<String, String> hm = new HashMap<String, String>();

	public static void main(String[] args) {
		Runner.runExample(Server.class);
	}

	@Override
	public void start() throws Exception {
		vertx.createHttpServer()
				.requestHandler(
						request -> {
							if (request.method().toString().equals("GET")) {
								if (request.path().equals("")
										|| request.path().equals("/")) {
									request.response()
											.putHeader("content-type",
													"text/html")
											.end("Welcome to cmucchackers!");
								} else if (request.path().equals("/q2")) {
									MultiMap queries = request.params();
									request.response()
											.putHeader("content-type",
													"text/html; charset=utf-8")
											.end(query(queries.get("userid"),
													queries.get("tweet_time")));
								} else {
									request.response()
											.putHeader("content-type",
													"text/html")
											.end("Wrong query!");
								}
							}
						}).listen(80);
	}

	private static String query(String userId, String tweetTime) {
		String teamName = "cmucchackers";
		String teamId = "363781473979";
		if (hm.containsKey(userId + tweetTime)) {
			return hm.get(userId + tweetTime);
		} else {
			int hashCode = EntryDAO.hashFunc(userId, tweetTime);
			// get the result from the data store with index starting from 0.
			// (same as hashCode)
			String tweets = get(hashCode, userId, tweetTime);
			String result = teamName + ',' + teamId + '\n' + tweets;
			hm.put(userId + tweetTime, result);
			return result;
		}
	}

	private static String get(int hashCode, String userId, String tweetTime) {
		StringBuilder sb = new StringBuilder();
		List<Entry> entries = entryDAO.getEntry(hashCode, userId, tweetTime);
		for (Entry entry : entries) {
			// String textDecoded =
			// StringEscapeUtils.unescapeJava(entry.getTweetText());
			String textDecoded = entry.getTweetText().replace("\\n", "\n")
					.replace("\\t", "\t").replace("\\r", "\r")
					.replace("\\\\", "\\");
			entry.setTweetText(textDecoded);
			sb.append(entry.toString());
		}
		return sb.toString();
	}
}
