import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.MultiMap;

/**
 * @author cmucchackers
 *
 */
public class Server extends AbstractVerticle {
	// Convenience method so you can run it in your IDE
	public static Hashtable<String, String> cache = new Hashtable<String, String>();
	private static int tableNum = 0;
	private static final int TABLE_CLIENT_NUMBER = 50000;

	public static void main(String[] args) throws IOException {
		Runner.runExample(Server.class);
	}

	public static final String teamInfo = "cmucchackers,363781473979\n";

	@Override
	public void start() throws Exception {
		vertx.createHttpServer()
				.requestHandler(
						request -> {
							tableNum++;
							tableNum %= TABLE_CLIENT_NUMBER;
							if (request.method().toString().equals("GET")) {
								if (request.path().equals("")
										|| request.path().equals("/")) {
									request.response()
											.putHeader("content-type",
													"text/html; charset=utf-8")
											.end("Welcome to cmucchackers!");
								} else if (request.path().equals("/q1")) {
									MultiMap queries = request.params();
									request.response()
											.putHeader("content-type",
													"text/html")
											.end(decipherMessage(
													queries.get("key"),
													queries.get("message")));
								} else if (request.path().equals("/q2")) {
									tableNum++;
									tableNum %= TABLE_CLIENT_NUMBER;
									MultiMap queries = request.params();
									try {
										request.response()
												.putHeader("content-type",
														"text/html; charset=utf-8")
												.end(HbaseServer.selectRowKey(
														queries.get("userid")
																+ queries
																		.get("tweet_time"),
														tableNum));
									} catch (Exception e) {
										e.printStackTrace();
									}
								}
							}
						}).listen(80);
	}

	public static String decipherMessage(String xy, String cipherText) {
		int xyLastTwoDigit = Integer.parseInt(xy.substring(xy.length() - 2,
				xy.length()));

		int[] keyArray = { 1, 13, 25, 12, 24, 11, 23, 10, 22, 9, 21, 8, 20, 7,
				19, 6, 18, 5, 17, 4, 16, 3, 15, 2, 14, 1, 13, 25, 12, 24, 11,
				23, 10, 22, 9, 21, 8, 20, 7, 19, 6, 18, 5, 17, 4, 16, 3, 15, 2,
				14, 1, 13, 25, 12, 24, 11, 23, 10, 22, 9, 21, 8, 20, 7, 19, 6,
				18, 5, 17, 4, 16, 3, 15, 2, 14, 1, 13, 25, 12, 24, 11, 23, 10,
				22, 9, 21, 8, 20, 7, 19, 6, 18, 5, 17, 4, 16, 3, 15, 2, 14 };
		int shift = keyArray[xyLastTwoDigit];
		int length = cipherText.length();
		int size = (int) Math.sqrt(length);
		Date date = new Date();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		String result = teamInfo + formatter.format(date) + "\n";
		for (int i = 0; i < 2 * size - 1; i++) {
			int length1 = i < size ? 0 : i - size + 1;
			int length2 = i < size ? 0 : i - size + 1;
			for (int j = i - length2; j >= length1; j--) {
				char c = (char) (cipherText.charAt(j + (i - j) * size) - shift);
				if (c < 65) {
					c += 26;
				}
				result += c;
			}
		}
		return result + "\n";
	};
}
