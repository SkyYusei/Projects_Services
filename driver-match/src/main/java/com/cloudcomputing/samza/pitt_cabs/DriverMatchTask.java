package com.cloudcomputing.samza.pitt_cabs;

import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.util.HashMap;

import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

/**
 * Consumes the stream of driver location updates and rider cab requests.
 * Outputs a stream which joins these 2 streams and gives a stream of rider to
 * driver matches.
 */
public class DriverMatchTask implements StreamTask, InitableTask,
		WindowableTask {

	/* Define per task state here. (kv stores etc) */
	private KeyValueStore<String, String> drivers;
	
	@Override
	@SuppressWarnings("unchecked")
	public void init(Config config, TaskContext context) throws Exception {
		// Initialize stuff (maybe the kv stores?)
		drivers = (KeyValueStore<String, String>) context.getStore("driver-loc");
	}

	@Override
	@SuppressWarnings("unchecked")
	public void process(IncomingMessageEnvelope envelope,
			MessageCollector collector, TaskCoordinator coordinator) {
		// The main part of your code. Remember that all the messages for a
		// particular partition
		// come here (somewhat like MapReduce). So for task 1 messages for a
		// blockId will arrive
		// at one task only, thereby enabling you to do stateful stream
		// processing.
		String incomingStream = envelope.getSystemStreamPartition().getStream();
		if (incomingStream.equals(DriverMatchConfig.EVENT_STREAM.getStream())) {
			processEvent((Map<String, Object>) envelope.getMessage(), collector);
		} else if (incomingStream.equals(DriverMatchConfig.DRIVER_LOC_STREAM.getStream())) {
			// do nothing
		} else {
			throw new IllegalStateException("Unexpected input stream: " + envelope.getSystemStreamPartition());
		}
	}

	private void processEvent(Map<String, Object> message,
			MessageCollector collector) {
		String type = (String) message.get("type");
		if (type.equals("ENTERING_BLOCK")) {
			String driverId = String.valueOf((Integer)message.get("driverId"));
			if (drivers.get(driverId) != null) {
				
			} else {
				String status = (String) message.get("status");
				String blockId = String.valueOf((Integer)message.get("blockId"));
				String latitude = String.valueOf((Integer)message.get("latitude"));
				String longitude = String.valueOf((Integer)message.get("longitude"));
				drivers.put(driverId, status + ":" + blockId + ":" + latitude + ":" + longitude);
			}
		} else if (type.equals("LEAVING_BLOCK")) {
			String driverId = String.valueOf((Integer)message.get("driverId"));
			if (drivers.get(driverId) != null) {
				
			} else {
				String status = (String)message.get("status");
				String blockId = String.valueOf((Integer)message.get("blockId"));
				String latitude = String.valueOf((Integer)message.get("latitude"));
				String longitude = String.valueOf((Integer)message.get("longitude"));
				drivers.put(driverId, status + ":" + blockId + ":" + latitude + ":" + longitude);
			}
		} else if (type.equals("RIDE_REQUEST")) {
			String riderId = String.valueOf((Integer)message.get("riderId"));
			String blockId = String.valueOf((Integer)message.get("blockId"));
			String latitude = String.valueOf((Integer)message.get("latitude"));
			String longitude = String.valueOf((Integer)message.get("longitude"));
			
			KeyValueIterator<String, String> driverList = drivers.all();

			double minDist = Double.MAX_VALUE;
			String minDriver = null, minLat = null, minLong = null;
			try {
				while (driverList.hasNext()) {
					String driverId = driverList.next().getKey();
					String[] words = drivers.get(driverId).split(":");
					String dstatus = words[0];
					String dblockId = words[1];
					String dlatitude = words[2];
					String dlongitude = words[3];
					
					if (dstatus.equals("AVAILABLE") && blockId.equals(dblockId)) {
						double newDist = distance(dlatitude, dlongitude, latitude, longitude);
						if (newDist < minDist) {
							minDist = newDist;
							minDriver = driverId;
							minLat = dlatitude;
							minLong = dlongitude;
						}
					}
				}
				if (minDriver != null) {
					drivers.put(minDriver, "UNAVAILABLE:" + blockId + ":" + minLat + ":" + minLong);
					Map<String, String> map = new HashMap<String, String>();
					map.put("riderId", riderId);
					map.put("driverId", minDriver);
					map.put("priceFactor", "1.0");
					collector.send(new OutgoingMessageEnvelope(DriverMatchConfig.MATCH_STREAM, map));
				}
			} finally {
				driverList.close();
			}
			
		} else if (type.equals("RIDE_COMPLETE")) {
			String driverId = String.valueOf((Integer)message.get("driverId"));
			if (drivers.get(driverId) != null) {
				
			} else {
				String blockId = String.valueOf((Integer)message.get("blockId"));
				String latitude = String.valueOf((Integer)message.get("latitude"));
				String longitude = String.valueOf((Integer)message.get("longitude"));
				drivers.put(driverId, "AVAILABLE" + ":" + blockId + ":" + latitude + ":" + longitude);
			}
		} else {
			throw new IllegalStateException("Unexpected input stream.");
		}
	}
	
	private double distance(String slat1, String slong1, String slat2, String slong2) {
		double lat1 = Double.parseDouble(slat1);
		double long1 = Double.parseDouble(slong1);
		double lat2 = Double.parseDouble(slat2);
		double long2 = Double.parseDouble(slong2);
		double dist1 = Math.abs(long2 - long1);
		double dist2 = Math.abs(lat2 - lat1);
		double dist = Math.sqrt(dist1 * dist1 + dist2 * dist2);
		return dist;
	}

	@Override
	public void window(MessageCollector collector, TaskCoordinator coordinator) {
		// this function is called at regular intervals, not required for this
		// project
	}
}
