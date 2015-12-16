import io.vertx.core.AbstractVerticle;
import io.vertx.core.MultiMap;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

public class Server extends AbstractVerticle {
	private static final String TEAM_INFO = "cmucchackers,363781473979\n";

	private static JDBCClient client;
	private static Map<String, ConcurrentHashMap<String, String>> q6table = new ConcurrentHashMap<String, ConcurrentHashMap<String, String>>();
	private static Map<String, PriorityBlockingQueue<String>> q6transaction = new ConcurrentHashMap<String, PriorityBlockingQueue<String>>();

	public static void main(String[] args) {
		Runner.runExample(Server.class);
	}

	@Override
	public void start() throws Exception {
		client = JDBCClient.createShared(vertx, new JsonObject()
		.put("url", "jdbc:mysql://localhost:3306/twitter?useUnicode=true&characterEncoding=utf-8")
		.put("driver_class", "com.mysql.jdbc.Driver")
		.put("user", "zhaoru")
		.put("password", "123456")
		.put("max_pool_size", 500));

		vertx.createHttpServer().requestHandler(request -> {

			if (request.method().toString().equals("GET")) {
				if (request.path().equals("") || request.path().equals("/")) {
					request.response().putHeader("content-type", "text/html").end("Welcome to cmucchackers!");
				} else if (request.path().equals("/q1")) {
					MultiMap queries = request.params();
					request.response().putHeader("content-type", "text/html")
					.end(decipherMessage(queries.get("key"), queries.get("message")));
				} else if (request.path().equals("/q2")) {
					MultiMap queries = request.params();
					String userId = queries.get("userid");
					String tweetTime = queries.get("tweet_time");
					String key = userId + tweetTime;
					client.getConnection(res -> {
						if (res.succeeded()) {
							SQLConnection connection = res.result();

							connection.query("SELECT q2value FROM q2 WHERE q2key = '" + key + "'", res2 -> {
								if (res2.succeeded()) {

									ResultSet rs = res2.result();
									String tweets = rs.getResults().get(0).getString(0);
									String result = TEAM_INFO + tweets + "\n";
									request.response().putHeader("content-type", "text/html; charset=utf-8").end(result);
								}
							});
							connection.close();
						} else {
							System.out.println("res not success.");
						}
					});

				}
				else if (request.path().equals("/q3")) {
					MultiMap queries = request.params();
					String startDate = queries.get("start_date");
					String endDate = queries.get("end_date");
					String userid = queries.get("userid");
					int n = Integer.parseInt(queries.get("n"));
					client.getConnection(res -> {
						if (res.succeeded()) {
							SQLConnection connection = res.result();

							connection.query("SELECT q3value FROM q3 WHERE q3key = '" + userid + "'", res2 -> {
								if (res2.succeeded()) {

									ResultSet rs = res2.result();
									String tweets = null;
									try {
										tweets = rs.getResults().get(0).getString(0);
									} catch (Exception e) {
										tweets = "";
									}

									String[] results = tweets.split("\\[cmucchackers\\]");
									ArrayList<String> positive = new ArrayList<String>();
									ArrayList<String> negative = new ArrayList<String>();

									if (!results[0].equals("")) {
										int i = 0;
										int len = results.length;
										while(positive.size() < n) {
											// try {
											if (i >= len) {
												break;
											}
											if (results[i].charAt(11) == '-') {
												break;
											}
											// } catch (Exception e) {
											//   System.out.println("*****************qian");
											//   System.out.println("Error: " + results[i]);
											//   System.out.println("Length: " + results.length);
											//   System.out.println("UserId: " + userid);
											//   System.out.println("*****************");
											// }
											String currentDate = results[i].substring(0,10);
											if (currentDate.compareTo(startDate) >= 0 && currentDate.compareTo(endDate) <= 0) {
												positive.add(results[i]);
											}
											i++;
										}
										int j = 0;

										while(negative.size() < n) {
											// try {
											if (j >= len) {
												break;
											}
											if (results[len - 1 - j].charAt(11) != '-') {
												break;
											}
											// } catch (Exception e) {
											//   System.out.println("*****************hou");
											//   System.out.println("Error: " + results[len - 1 - j]);
											//   System.out.println("Length: " + results.length);
											//   System.out.println("UserId: " + userid);
											//   System.out.println("*****************");
											// }

											String currentDate = results[len - 1 - j].substring(0,10);
											if (currentDate.compareTo(startDate) >= 0 && currentDate.compareTo(endDate) <= 0) {
												negative.add(results[len-1-j]);
											}
											j++;
										}
									}
									StringBuilder sb = new StringBuilder();
									sb.append(TEAM_INFO);
									sb.append("Positive Tweets\n");
									for(String s : positive) {
										sb.append(s + "\n");
									}
									sb.append("\n");
									sb.append("Negative Tweets\n");
									for(String s : negative) {
										sb.append(s + "\n");
									}

									request.response().putHeader("content-type", "text/html; charset=utf-8").end(sb.toString());
								}
							});
							connection.close();
						} else {
							System.out.println("res not success.");
						}
					});

				} else if (request.path().equals("/q4")) {
					MultiMap queries = request.params();
					String hashtag = queries.get("hashtag");
					int n = Integer.parseInt(queries.get("n"));
					// System.out.println(hashtag + " " + n);
					client.getConnection(res -> {
						if (res.succeeded()) {
							SQLConnection connection = res.result();

							connection.query("SELECT q4value FROM q4 WHERE q4key = '" + hashtag + "'", res2 -> {
								if (res2.succeeded()) {
									String result = "";
									ResultSet rs = res2.result();
									if (rs.getResults().size() != 0) {
										result = rs.getResults().get(0).getString(0);
										String[] resultArray = result.split("\\[cmucchackers\\]");
										int count = 0;
										result = TEAM_INFO;
										while (count < n && count < resultArray.length) {
											result += resultArray[count] + "\n";
											count++;
										}
									}
									request.response().putHeader("content-type", "text/html; charset=utf-8").end(result);
								}
							});
							connection.close();
						} else {
							System.out.println("res not success.");
						}
					});

				} else if (request.path().equals("/q5")) {
					MultiMap queries = request.params();
					client.getConnection(res -> {
						if (res.succeeded()) {

							SQLConnection connection = res.result();

							String userid_min_string = queries.get("userid_min");
							String userid_max_string = queries.get("userid_max");
							int userid_min = 0;
							try {
								userid_min = Integer.parseInt(userid_min_string);
							} catch (Exception e) {
								userid_min = 2;
							}
							if (userid_min < 12) {
								userid_min = 2;
							}
							// String sql1 = "SELECT tweet_num FROM q5Table WHERE user_id > " + userid_min + " OEDER BY user_id DESC LIMIT 1";
							connection.query("SELECT q5value FROM q5 WHERE q5key = "
									+ "(SELECT MAX(q5key) FROM q5 WHERE q5key < " + userid_min +
									") OR q5key = (SELECT MAX(q5key) FROM q5 WHERE q5key <= " + userid_max_string + ")", res2 -> {
										if (res2.succeeded()) {
											ResultSet rs = res2.result();
											String num1 = rs.getResults().get(0).getString(0);

											String num2 = rs.getResults().get(1).getString(0);

											int number = Integer.parseInt(num2) - Integer.parseInt(num1);
											String result = TEAM_INFO + number + "\n";
											request.response().putHeader("content-type", "text/html; charset=utf-8").end(result);
										}
									});
							connection.close();
						} else {
							System.out.println("res not success.");
						}
					});
				} else if (request.path().equals("/q6")) {
					MultiMap queries = request.params();
					String tid = queries.get("tid");
					String opt = queries.get("opt");
					String result = TEAM_INFO;
					if (opt.equals("s")) {
//						q6table.put(tid, new ConcurrentHashMap<String, String>());
//						q6transaction.put(tid, new PriorityBlockingQueue<String>());
						request.response().putHeader("content-type", "text/html; charset=utf-8")
						.end(result + "0\n");
					} else if (opt.equals("e")) {
						request.response().putHeader("content-type", "text/html; charset=utf-8")
						.end(result + "0\n");
						try {
							Thread.sleep(10 * 1000);
							q6table.remove(tid);
							q6transaction.remove(tid);
						} catch (Exception e) {
							e.printStackTrace();
						}
						
					} else {
						client.getConnection(res -> {
							if (res.succeeded()) {

								SQLConnection connection = res.result();

								if (opt.equals("a")) {
									String tweetid = queries.get("tweetid");
									String tag = queries.get("tag");
									String seq = queries.get("seq");
									if (!q6transaction.containsKey(tid)) {
										q6table.put(tid, new ConcurrentHashMap<String, String>());
										q6transaction.put(tid, new PriorityBlockingQueue<String>());
									}
									synchronized (q6transaction.get(tid)) {
										
										q6transaction.get(tid).add(seq);
										while (!seq.equals(q6transaction.get(tid).peek())) {
											try {
												q6transaction.get(tid).wait();
											} catch (InterruptedException e) {
												e.printStackTrace();
											}
										}
									}
									connection.query("SELECT q6value FROM q6 WHERE q6key = '" + tweetid + "';", res2 -> {
										if (res2.succeeded()) {
											ResultSet rs = res2.result();

											String text = rs.getResults().get(0).getString(0);
											text = text + tag;

											q6table.get(tid).put(tweetid, text);
											String theResult = TEAM_INFO + tag + "\n";
											request.response().putHeader("content-type", "text/html; charset=utf-8")
											.end(theResult);

											// connection.query("INSERT INTO q6s
											// VALUES ('" + tweetid + "','" + text +
											// "') ON DUPLICATE KEY UPDATE q6value =
											// '" + text + "';", res3 -> {
											// String result = TEAM_INFO + tag +
											// "\n";
											// request.response().putHeader("content-type",
											// "text/html;
											// charset=utf-8").end(result);
											// });
										}
									});
									q6transaction.get(tid).poll();
									q6transaction.get(tid).notifyAll();
								} else if (opt.equals("r")) {
									String tweetid = queries.get("tweetid");
									String seq = queries.get("seq");
									if (!q6transaction.containsKey(tid)) {
										q6table.put(tid, new ConcurrentHashMap<String, String>());
										q6transaction.put(tid, new PriorityBlockingQueue<String>());
									}
									synchronized (q6transaction.get(tid)) {
										q6transaction.get(tid).add(seq);
										while (!seq.equals(q6transaction.get(tid).peek())) {
											try {
												q6transaction.get(tid).wait();
											} catch (InterruptedException e) {
												e.printStackTrace();
											}
										}
									}
									if (q6table.get(tid).containsKey(tweetid)) {
										String text = q6table.get(tid).get(tweetid);
										String theResult = TEAM_INFO + text + "\n";
										request.response().putHeader("content-type", "text/html; charset=utf-8")
										.end(theResult);
									} else {
										connection.query("SELECT q6value FROM q6 WHERE q6key = '" + tweetid + "';",
												res4 -> {
													if (res4.succeeded()) {
														ResultSet rs = res4.result();
														String text = rs.getResults().get(0).getString(0);
														String theResult = TEAM_INFO + text + "\n";
														q6table.get(tid).put(tweetid, text);
														request.response().putHeader("content-type", "text/html; charset=utf-8")
														.end(theResult);
													}
												});
									}
									q6transaction.get(tid).poll();
									q6transaction.get(tid).notifyAll();
								} 

								connection.close();
							} else {
								System.out.println("res not success.");
							}
						});
					}
				} else {
					request.response().putHeader("content-type", "text/html").end("Wrong query!");
				}
			}
		}).listen(80);
	}

	public static String decipherMessage(String xy, String cipherText) {
		int xyLastTwoDigit = Integer.parseInt(xy.substring(xy.length() - 2, xy.length()));

		int[] keyArray = { 1, 13, 25, 12, 24, 11, 23, 10, 22, 9, 21, 8, 20, 7, 19, 6, 18, 5, 17, 4, 16, 3, 15, 2, 14, 1,
				13, 25, 12, 24, 11, 23, 10, 22, 9, 21, 8, 20, 7, 19, 6, 18, 5, 17, 4, 16, 3, 15, 2, 14, 1, 13, 25, 12,
				24, 11, 23, 10, 22, 9, 21, 8, 20, 7, 19, 6, 18, 5, 17, 4, 16, 3, 15, 2, 14, 1, 13, 25, 12, 24, 11, 23,
				10, 22, 9, 21, 8, 20, 7, 19, 6, 18, 5, 17, 4, 16, 3, 15, 2, 14 };
		int shift = keyArray[xyLastTwoDigit];
		int length = cipherText.length();
		int size = (int) Math.sqrt(length);
		Date date = new Date();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		String result = TEAM_INFO + formatter.format(date) + "\n";
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

