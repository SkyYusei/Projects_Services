import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Map;
import java.util.TimeZone;
import java.util.Iterator;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.sql.Timestamp;

import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

public class Coordinator extends Verticle {

	//Default mode: replication. Possible string values are "replication" and "sharding"
	private static String storageType = "replication";

	/**
	 * TODO: Set the values of the following variables to the DNS names of your
	 * three dataCenter instances
	 */
	private static final String dataCenter1 = "ec2-54-152-245-246.compute-1.amazonaws.com";
	private static final String dataCenter2 = "ec2-54-173-208-139.compute-1.amazonaws.com";
	private static final String dataCenter3 = "ec2-54-174-250-180.compute-1.amazonaws.com";
	
	// The map to store each key and a queue for the timestamp to reqesting that key
	private Map<String, BlockingQueue<String>> table = new Hashtable<String, BlockingQueue<String>>();

	@Override
	public void start() {
		//DO NOT MODIFY THIS
		KeyValueLib.dataCenters.put(dataCenter1, 1);
		KeyValueLib.dataCenters.put(dataCenter2, 2);
		KeyValueLib.dataCenters.put(dataCenter3, 3);
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
				//You may use the following timestamp for ordering requests
                                final String timestamp = new Timestamp(System.currentTimeMillis() 
                                                                + TimeZone.getTimeZone("EST").getRawOffset()).toString();
				Thread t = new Thread(new Runnable() {
					public void run() {
						//TODO: Write code for PUT operation here.
						//Each PUT operation is handled in a different thread.
						//Highly recommended that you make use of helper functions.
						BlockingQueue<String> queue = null;
						// building or getting the queue
						if (table.get(key) != null) {
							queue = table.get(key);
							queue.add(timestamp);
						} else {
							queue = new PriorityBlockingQueue<String>();
							queue.add(timestamp);
							table.put(key, queue);
						}
						// lock the queue when modifing the queue
						synchronized (queue) {
							String topTS = queue.peek();
							// let the request wait when it is not the newest request
							while (!timestamp.equals(topTS)) {
								try {
									queue.wait();
									topTS = queue.peek();
								} catch (InterruptedException e) {
									e.printStackTrace();
								}
							}
							// for replication
							if (storageType.equals("replication")) {
								try {
									KeyValueLib.PUT(dataCenter1, key, value);
									KeyValueLib.PUT(dataCenter2, key, value);
									KeyValueLib.PUT(dataCenter3, key, value);
								} catch (Exception e) {
									e.printStackTrace();
								}
							} else if (storageType.equals("sharding")) {
								// for sharding
								int hash = hashFunction(key);
								try {
									if (hash == 0) {
										KeyValueLib.PUT(dataCenter1, key, value);
									} else if (hash == 1) {
										KeyValueLib.PUT(dataCenter2, key, value);
									} else if (hash == 2) {
										KeyValueLib.PUT(dataCenter3, key, value);
									}
								} catch (Exception e) {
									e.printStackTrace();
								}
							}
							// poll the timestamp when the request is done
							queue.poll();
							// notify all waiting requests
							queue.notifyAll();
						}
					}
				});
				t.start();
				req.response().end(); //Do not remove this
			}
		});

		routeMatcher.get("/get", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				final String loc = map.get("loc");
				//You may use the following timestamp for ordering requests
				final String timestamp = new Timestamp(System.currentTimeMillis() 
								+ TimeZone.getTimeZone("EST").getRawOffset()).toString();
				Thread t = new Thread(new Runnable() {
					public void run() {
						//TODO: Write code for GET operation here.
						//Each GET operation is handled in a different thread.
						//Highly recommended that you make use of helper functions.
						String result = null;
						// building or getting the queue
						BlockingQueue<String> queue = null;
						if (table.get(key) != null) {
							queue = table.get(key);
							queue.add(timestamp);
						} else {
							queue = new PriorityBlockingQueue<String>();
							queue.add(timestamp);
							table.put(key, queue);
						}
						// lock the queue when modifing the queue
						synchronized (queue) {
							String topTS = queue.peek();
							// let the request wait when it is not the newest request
							while (!timestamp.equals(topTS)) {
								try {
									queue.wait();
									topTS = queue.peek();
								} catch (InterruptedException e) {
									e.printStackTrace();
								}
							}
							// for replication
							if (storageType.equals("replication")) {
								try {
									result = KeyValueLib.GET(dataCenter1, key);
									KeyValueLib.GET(dataCenter2, key);
									KeyValueLib.GET(dataCenter3, key);
								} catch (Exception e) {
									e.printStackTrace();
								}
							} else if (storageType.equals("sharding")) {
								// for sharding
								int hash = hashFunction(key);
								try {
									// if the loc variable exists
									if (loc != null) {
										if (loc.equals("1") && hash == 0) {
											result = KeyValueLib.GET(dataCenter1, key);
										} else if (loc.equals("2") && hash == 1) {
											result = KeyValueLib.GET(dataCenter2, key);
										} else if (loc.equals("3") && hash == 2) {
											result = KeyValueLib.GET(dataCenter3, key);
										} else {
											// the loc does not match the hashed key
											result = "0";
										}
									} else {
										// if the loc variable does not exist
										if (hash == 0) {
											result = KeyValueLib.GET(dataCenter1, key);
										} else if (hash == 1) {
											result = KeyValueLib.GET(dataCenter2, key);
										} else if (hash == 2) {
											result = KeyValueLib.GET(dataCenter3, key);
										}
									}
								} catch (Exception e) {
									e.printStackTrace();
								}
							}
							// poll the timestamp when the request is done
							queue.poll();
							// notify all waiting requests
							queue.notifyAll();
						}		
						req.response().end(result); //Default response = 0
					}
				});
				t.start();
			}
		});

		routeMatcher.get("/storage", new Handler<HttpServerRequest>() {
                        @Override
                        public void handle(final HttpServerRequest req) {
                                MultiMap map = req.params();
                                storageType = map.get("storage");
                                //This endpoint will be used by the auto-grader to set the 
				//consistency type that your key-value store has to support.
                                //You can initialize/re-initialize the required data structures here
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
		char[] array = key.toCharArray();
		int sum = 0;
		int length = array.length;
		for (char c : array) {
			int asc = (int)c;
			sum = sum + asc / length;
		}
		sum = Math.abs(sum);
		return (int)sum % 3;
	}
}
