import java.io.IOException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Queue;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

public class Coordinator extends Verticle {
	// This integer variable tells you what region you are in
	// 1 for US-E, 2 for US-W, 3 for Singapore
	private static int region = KeyValueLib.region;

	// Default mode: Strongly consistent
	// Options: causal, eventual, strong
	private static String consistencyType = "strong";

	/**
	 * TODO: Set the values of the following variables to the DNS names of your
	 * three dataCenter instances. Be sure to match the regions with their DNS!
	 * Do the same for the 3 Coordinators as well.
	 */
	private static final String dataCenterUSE = "ec2-54-175-75-111.compute-1.amazonaws.com";
	private static final String dataCenterUSW = "ec2-52-23-154-29.compute-1.amazonaws.com";
	private static final String dataCenterSING = "ec2-52-91-139-186.compute-1.amazonaws.com";

	private static final String coordinatorUSE = "ec2-54-173-109-188.compute-1.amazonaws.com";
	private static final String coordinatorUSW = "ec2-52-91-33-53.compute-1.amazonaws.com";
	private static final String coordinatorSING = "ec2-54-172-226-128.compute-1.amazonaws.com";
	
	@Override
	public void start() {
		KeyValueLib.dataCenters.put(dataCenterUSE, 1);
		KeyValueLib.dataCenters.put(dataCenterUSW, 2);
		KeyValueLib.dataCenters.put(dataCenterSING, 3);
		KeyValueLib.coordinators.put(coordinatorUSE, 1);
		KeyValueLib.coordinators.put(coordinatorUSW, 2);
		KeyValueLib.coordinators.put(coordinatorSING, 3);
		final RouteMatcher routeMatcher = new RouteMatcher();
		final HttpServer server = vertx.createHttpServer();
		server.setAcceptBacklog(32767);
		server.setUsePooledBuffers(true);
		server.setReceiveBufferSize(4 * 1024);

		routeMatcher.get("/put", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				final String value = map.get("value");
				final Long timestamp = Long.parseLong(map.get("timestamp"));
				final String forwarded = map.get("forward");
				final String forwardedRegion = map.get("region");
				
				final int hashCode = hashFunction(key) + 1;
				final String primaryCoor = getCoorbyInt(hashCode);
				final String currentCoor = getCoorbyInt(region);
				
				System.out.println("PUT " +key+" "+value+" "+timestamp);
				
				if (consistencyType.equals("strong") || consistencyType.equals("causal")) {
					Thread t = new Thread(new Runnable() {
						public void run() {
							if (forwarded != null && forwarded.equals("true")) {
								Skews.handleSkew(timestamp, Integer.parseInt(forwardedRegion));
								// no need to ahead again
								putAll(key, value, String.valueOf(timestamp));
							} else {
								if (hashCode != region) {
									try {
										KeyValueLib.AHEAD(key, String.valueOf(timestamp));
										System.out.println("hashcode: " + hashCode+"    PDC:"+primaryCoor);
										KeyValueLib.FORWARD(primaryCoor, key, value, String.valueOf(timestamp));
									} catch (IOException e) {
										e.printStackTrace();
									}
								} else {
									try {
										KeyValueLib.AHEAD(key, String.valueOf(timestamp));
										putAll(key, value, String.valueOf(timestamp));
									} catch (IOException e) {
										e.printStackTrace();
									}
								}
							}							
							/* TODO: Add code for PUT request handling here
							 * Each operation is handled in a new thread.
							 * Use of helper functions is highly recommended */
						}
					});
					t.start();
				} else if (consistencyType.equals("eventual")) {
					if (forwarded != null && forwarded.equals("true")) {
						Skews.handleSkew(timestamp, Integer.parseInt(forwardedRegion));
						putAll(key, value, String.valueOf(timestamp));
					} else {
						if (hashCode != region) {
							try {
								KeyValueLib.FORWARD(primaryCoor, key, value, String.valueOf(timestamp));
							} catch (IOException e) {
								e.printStackTrace();
							}
						} else {
							putAll(key, value, String.valueOf(timestamp));
						}
					}
//					putAll(key, value, String.valueOf(timestamp));
				} else {
					if (forwarded != null && forwarded.equals("true")) {
						Skews.handleSkew(timestamp, Integer.parseInt(forwardedRegion));
						putAll(key, value, String.valueOf(timestamp));
					} else {
						if (hashCode != region) {
							try {
								KeyValueLib.FORWARD(primaryCoor, key, value, String.valueOf(timestamp));
							} catch (IOException e) {
								e.printStackTrace();
							}
						} else {
							putAll(key, value, String.valueOf(timestamp));
						}
					}
				}		
				req.response().end(); // Do not remove this
			}
		});

		routeMatcher.get("/get", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				final Long timestamp = Long.parseLong(map.get("timestamp"));
				
				System.out.println("GET " +key+" "+timestamp);
				
				if (consistencyType.equals("strong")) {
					Thread t = new Thread(new Runnable() {
						public void run() {
							String value = "0";
							String currentDC = getDCbyInt(region);
							try {
								KeyValueLib.AHEAD(key, String.valueOf(timestamp));
								value = KeyValueLib.GET(currentDC, key, String.valueOf(timestamp), consistencyType);
							} catch (IOException e) {
								e.printStackTrace();
							}
						/* TODO: Add code for GET requests handling here
						 * Each operation is handled in a new thread.
						 * Use of helper functions is highly recommended */
							if (value.trim().equals("")) value = "0";
							String response = value;
							req.response().end(response);
						}
					});
					t.start();
				} else if (consistencyType.equals("causal") || consistencyType.equals("eventual")) {
					new Thread(new Runnable() {
						
						@Override
						public void run() {
							String value = "0";
							String currentDC = getDCbyInt(region);
							try {
								value = KeyValueLib.GET(currentDC, key, String.valueOf(timestamp), consistencyType);
							} catch (IOException e) {
								e.printStackTrace();
							}
							if (value.trim().equals("")) value = "0";
							String response = value;
							req.response().end(response);
						}
					}).start();
				} else {
					new Thread(new Runnable() {
						
						@Override
						public void run() {
							String value = "0";
							String currentDC = getDCbyInt(region);
							try {
								value = KeyValueLib.GET(currentDC, key, String.valueOf(timestamp), consistencyType);
							} catch (IOException e) {
								e.printStackTrace();
							}
							if (value.trim().equals("")) value = "0";
							String response = value;
							req.response().end(response);
						}
					}).start();
				}				
			}
		});
		/* This endpoint is used by the grader to change the consistency level */
		routeMatcher.get("/consistency", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				consistencyType = map.get("consistency");
				req.response().end();
			}
		});
		/* BONUS HANDLERS BELOW */
		routeMatcher.get("/forwardcount", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				req.response().end(KeyValueLib.COUNT());
			}
		});

		routeMatcher.get("/reset", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				KeyValueLib.RESET();
				req.response().end();
			}
		});

		routeMatcher.noMatch(new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				req.response().putHeader("Content-Type", "text/html");
				String response = "Not found.";
				req.response().putHeader("Content-Length",
						String.valueOf(response.length()));
				req.response().end(response);
				req.response().close();
			}
		});
		server.requestHandler(routeMatcher);
		server.listen(8080);
	}
	
	private int hashFunction(String key) {
		if (key.equals("a")) return 0;
		if (key.equals("b")) return 1;
		if (key.equals("c")) return 2;
		int result = 0;
		for (int i = 0; i < key.length(); i++) {
			result = result + 31 * key.charAt(i);
		}
		return Math.abs(result) % 3;
	}
	
	private String getCoorbyInt(int hashCode) {
		String dataCenter = null;
		if (hashCode == 1) dataCenter = coordinatorUSE;
		else if (hashCode == 2) dataCenter = coordinatorUSW;
		else if (hashCode == 3) dataCenter = coordinatorSING;
		return dataCenter;
	}
	
	private String getDCbyInt(int hashCode) {
		String dataCenter = null;
		if (hashCode == 1) dataCenter = dataCenterUSE;
		else if (hashCode == 2) dataCenter = dataCenterUSW;
		else if (hashCode == 3) dataCenter = dataCenterSING;
		return dataCenter;
	}
	
	private void putAll(final String key, final String value, final String timestamp) {
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					KeyValueLib.PUT(dataCenterUSE, key, value, timestamp, consistencyType);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}).start();
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					KeyValueLib.PUT(dataCenterUSW, key, value, timestamp, consistencyType);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}).start();
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					KeyValueLib.PUT(dataCenterSING, key, value, timestamp, consistencyType);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}).start();
	}
}
