import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.TimeZone;
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

public class KeyValueStore extends Verticle {
	private static HashMap<String, ArrayList<StoreValue>> store = null;
	private Map<String, BlockingQueue<Long>> table = null;

	public KeyValueStore() {
		store = new HashMap<String, ArrayList<StoreValue>>();
		table = new Hashtable<String, BlockingQueue<Long>>();
	}

	@Override
	public void start() {
		final KeyValueStore keyValueStore = new KeyValueStore();
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
				final String consistency = map.get("consistency");
				final Integer region = Integer.parseInt(map.get("region"));
								
				Long timestamp = Long.parseLong(map.get("timestamp"));
				/* TODO: You will need to adjust the timestamp here for some consistency levels */
				timestamp = Skews.handleSkew(timestamp, region); // change to others later
				final StoreValue sv = new StoreValue(timestamp, value);

				/* TODO: Add code to store the object here. You may need to adjust the timestamp */

				if (consistency.equals("strong") || consistency.equals("causal")) {
					BlockingQueue<Long> queue = table.get(key);

					synchronized (queue) {
						long topTS = queue.peek();
						// let the request wait when it is not the newest request
						while (!timestamp.equals(topTS)) {
							try {
								queue.wait();
								topTS = queue.peek();
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
						}
						addItemToStore(key, sv);
						// poll the timestamp when the request is done
						queue.poll();
						// notify all waiting requests
						queue.notifyAll();
					}
				} else if (consistency.equals("eventual")) {
					new Thread(new Runnable() {
						@Override
						public void run() {
							addItemToStore(key, sv);
							String response = "stored";
							req.response().putHeader("Content-Type", "text/plain");
							req.response().putHeader("Content-Length",
									String.valueOf(response.length()));
							req.response().end(response);
							req.response().close();
						}
					}).start();
				} else {
					new Thread(new Runnable() {
						@Override
						public void run() {
							addItemToStore(key, sv);
							String response = "stored";
							req.response().putHeader("Content-Type", "text/plain");
							req.response().putHeader("Content-Length",
									String.valueOf(response.length()));
							req.response().end(response);
							req.response().close();
						}
					}).start();
				}
				
				
			}
		});
		routeMatcher.get("/get", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				String consistency = map.get("consistency");
				final Long timestamp = Long.parseLong(map.get("timestamp"));
								
				/* TODO: Add code here to get the list of StoreValue associated with the key 
				 * Remember that you may need to implement some locking on certain consistency levels */
				
				if (consistency.equals("strong")) {
					ArrayList<StoreValue> values = null;
					BlockingQueue<Long> queue = table.get(key);

					synchronized (queue) {
						long topTS = queue.peek();
						// let the request wait when it is not the newest request
						while (!timestamp.equals(topTS)) {
							try {
								queue.wait();
								topTS = queue.peek();
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
						}
						values = store.get(key);
						// poll the timestamp when the request is done
						queue.poll();
						// notify all waiting requests
						queue.notifyAll();
					}
					String response = "";
					if (values != null) {
						for (StoreValue val : values) {
							response = response + val.getValue() + " ";
						}
					}
					req.response().putHeader("Content-Type", "text/plain");
					if (response != null)
						req.response().putHeader("Content-Length",
								String.valueOf(response.length()));
					req.response().end(response);
					req.response().close();
				} else if (consistency.equals("causal") || consistency.equals("eventual")) {
					new Thread(new Runnable() {
						
						@Override
						public void run() {
							ArrayList<StoreValue> values = null;
							values = store.get(key);
							values = adjust(values);
							String response = "";
							if (values != null) {
								for (StoreValue val : values) {
									response = response + val.getValue() + " ";
								}
							}
							req.response().putHeader("Content-Type", "text/plain");
							if (response != null)
								req.response().putHeader("Content-Length",
										String.valueOf(response.length()));
							req.response().end(response);
							req.response().close();
						}
					}).start();					
				} else {
					new Thread(new Runnable() {

						@Override
						public void run() {
							ArrayList<StoreValue> values = null;
							values = store.get(key);
							values = adjust(values);
							String response = "";
							if (values != null) {
								for (StoreValue val : values) {
									response = response + val.getValue() + " ";
								}
							}
							req.response().putHeader("Content-Type", "text/plain");
							if (response != null)
								req.response().putHeader("Content-Length",
										String.valueOf(response.length()));
							req.response().end(response);
							req.response().close();
						}
					}).start();	
				}

				/* Do NOT change the format the response. It will return a string of
				 * values separated by spaces */
			}
		});
		// Handler for when the AHEAD is called
		routeMatcher.get("/ahead", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				String key = map.get("key");
				final Long timestamp = Long.parseLong(map.get("timestamp"));
				
				addItemToTable(key, timestamp);
				
				req.response().putHeader("Content-Type", "text/plain");
				req.response().end();
				req.response().close();
			}
		});
		// Handler for when the COMPLETE is called
		routeMatcher.get("/complete", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				String key = map.get("key");
				final Long timestamp = Long.parseLong(map.get("timestamp"));
				req.response().putHeader("Content-Type", "text/plain");
				req.response().end();
				req.response().close();
			}
		});
		// Clears this stored keys. Do not change this
		routeMatcher.get("/reset", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				keyValueStore.store.clear();
				req.response().putHeader("Content-Type", "text/plain");
				req.response().end();
				req.response().close();
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
	private void addItemToStore(final String key, final StoreValue sv) {
		ArrayList<StoreValue> array;
		if (store.get(key) != null) {
			array = store.get(key);
			if (array == null) {
				array = new ArrayList<StoreValue>();
			}
		} else {
			array = new ArrayList<StoreValue>();
		}
		array.add(sv);
		Collections.sort(array, new Comparator<StoreValue>() {
			@Override
			public int compare(StoreValue sv1, StoreValue sv2) {
				return (int) (sv1.getTimestamp() - sv2.getTimestamp());
			}
		});
		store.put(key, array);
	}
	
	private ArrayList<StoreValue> adjust(ArrayList<StoreValue> array) {
		if (array != null) {
			Collections.sort(array, new Comparator<StoreValue>() {
				@Override
				public int compare(StoreValue sv1, StoreValue sv2) {
					return (int) (sv1.getTimestamp() - sv2.getTimestamp());
				}
			});
		}
		return array;
	}
	
	private BlockingQueue<Long> addItemToTable(final String key, final Long timestamp) {
		BlockingQueue<Long> queue = null;
		// building or getting the queue
		if (table.get(key) != null) {
			queue = table.get(key);
			if (queue == null) {
				queue = new PriorityBlockingQueue<Long>();
			}
		} else {
			queue = new PriorityBlockingQueue<Long>();
		}
		queue.add(timestamp);
		table.put(key, queue);
		
		return queue;
	}
}
