import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.Tag;

public class LoadBalancer {
	private static final int THREAD_POOL_SIZE = 10;
	private final ServerSocket socket;

	// used to run below methods
	private BasicAWSCredentials bawsc;
	private AmazonEC2Client ec2;
	
	// three variables to track instances
	private DataCenterInstance[] instances;
	private boolean[] isHealthy = new boolean[] {true, true, true};
	private int healthCount = 3;
	private boolean needAddInstance = false, isAddingInstance = false;

	public LoadBalancer(ServerSocket socket, DataCenterInstance[] instances) throws IOException {
		this.socket = socket;
		this.instances = instances;
		bawsc = loadProperties();
		ec2 = new AmazonEC2Client(bawsc);
	}

	// Complete this function
	public void start() throws IOException {
		Random random = new Random();
		int index = 0;
		ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
		
		Thread healthCheckThread = new Thread(new RunHealthCheckClass());
		healthCheckThread.start();
		
		while (true) {			
			if (healthCount == 3) needAddInstance = false;
			if (healthCount < 3) {
				if (isAddingInstance == true) {
					needAddInstance = false;
				} else {
					needAddInstance = true;
				}
			}
			
			if (needAddInstance == true) {
				// flag to show we are adding the instance, which will be false
				// after the runInstanceThread is done
				isAddingInstance = true;
				// we do not need to add instance even now healthCount is lass than 3
				needAddInstance = false;
				
				System.out.println("current health count: " + healthCount);
				System.out.println("Run instance thread start.");
				
				Thread runInstanceThread = new Thread(new RunInstanceClass());
				runInstanceThread.start();
			}
			
			Runnable requestHandler = new RequestHandler(socket.accept(), instances[index]);
			executorService.execute(requestHandler);
			
			index = nextIndexRR(index);
		}
	}

	/**
	 * Runnable class to run a health check
	 * 
	 * @author zhaoru
	 *
	 */
	private class RunHealthCheckClass implements Runnable {
		@Override
		public void run() {
			while (true) {
				healthCheck();
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * Runnable class to run and wait for a new instance
	 * 
	 * @author zhaoru
	 *
	 */
	private class RunInstanceClass implements Runnable {
		@Override
		public void run() {			
			// set up new data center
			RunInstancesRequest dataCenterRequest = createDataCenterRequest();
			RunInstancesResult runDataCenterResult = ec2.runInstances(dataCenterRequest);
			Instance dataCenterInstance = runDataCenterResult.getReservation().getInstances().get(0);
			String dataCenterId = dataCenterInstance.getInstanceId();
			createTag(ec2, dataCenterInstance);
			
			// wait for 1 min for the data center running
			try {
				Thread.sleep(60 * 1000);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			
			// wait for data center ready for the web server
			String testSentence = null;
			while (testSentence == null) {
				System.out.println("RunInstance Thread: Waiting for data center ready");
				
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
				List<Instance> allInstances = listInstance(ec2);
				for (Instance inst : allInstances) {
					if (inst.getInstanceId().equals(dataCenterId)) {
						dataCenterInstance = inst;
						break;
					}
				}

				String randomUserUrl = "http://"
						+ dataCenterInstance.getPublicDnsName()
						+ "/lookup/random";
				try {
					testSentence = sendGet(randomUserUrl);
				} catch (Exception e) {
					// Do nothing
				}
			}
			
			String newDataCenterUrl = "http://" + dataCenterInstance.getPublicDnsName();
			
			// update the data center to in-service instances
			for (int i = 0; i < 3; i++) {
				if (instances[i].getUrl() == null) {
					instances[i].changeUrl(newDataCenterUrl);
					isHealthy[i] = true;
					updataHealthCount();
				}
			}
			// complete adding instance
			isAddingInstance = false;
		}
	}
	
	private void healthCheck() {
		for (int i = 0; i < 3; i++) {
			if (instances[i].getUrl() != null) {
				String url = instances[i].getUrl() + "/lookup/random";
				int responseCode = 0;
				try {
					responseCode = getResponseCode(url);
				} catch (IOException e) {
					// error when connection to server, dc is dead
					System.out.println("error when connection to server, dc is dead");
					instances[i].changeUrl(null);
					isHealthy[i] = false;
					continue;
				}
				if (responseCode != 200) {
					System.out.println("dead dc response code is not 200");
					instances[i].changeUrl(null);
					isHealthy[i] = false;
				}
			}
		}		
		updataHealthCount();
	}

	// next index with round robin method
	private int nextIndexRR(int currentIndex) {
		int count = 0;
		while (true) {
			currentIndex++;
			currentIndex = currentIndex % 3;
			if (isHealthy[currentIndex] == true) {
				return currentIndex;
			}
			count++;
			// should not happen
			if (count > 5) {
				return 0;
			}
		}
	}
	
	// get next index of instances to assign request, considering some dead instances
	private int nextIndex(int currentIndex, Random random) {
//		System.out.println(Arrays.toString(isHealthy));
//		System.out.println(healthCount);
		
		int nextIndex;
		if (healthCount == 3) {
//			System.out.println("Health count is 3");
			int factor = random.nextInt(20);
			if (factor < 10) nextIndex = (currentIndex + 2) % 3;
			else nextIndex = (currentIndex + 1) % 3;
		} else if (healthCount == 2) {
//			System.out.println("Health count is 2");
			// get indices of health instances
			int[] count = new int[2];
			int num = 0;
			for (int i = 0; i < 3; i++) {
				if (isHealthy[i] == true) {
					count[num] = i;
					num++;
				}
			}
			if (currentIndex == count[0]) nextIndex = count[1];
			else if (currentIndex == count[1]) nextIndex = count[0];
			else nextIndex = count[0]; // should not happen, assign next to be either one
		} else if (healthCount == 1) {
			System.out.println("Health count is 1");
			// find the health index
			int num = 0;
			for (int j = 0; j < 3; j++) {
				if (isHealthy[j] == true) {
					num = j;
				}
			}
			// assign next index
			if (currentIndex == num) nextIndex = currentIndex;
			else nextIndex = num;
		} else {
			// Should not happen
			System.out.println("Health count is " + healthCount);
			return 0;
		}
		return nextIndex;
	}
	
	// updata the health count by the number of trues in isHealthy
	private void updataHealthCount() {
		int count = 0;
		for (boolean b : isHealthy) {
			if (b == true) count++;
		}
		healthCount = count;
	}
	
	// check cpu and assign which index to receive the request
	private int getMinIndex(int count, int minIndex, Double[] cpu, Random random) {
		// check cpu one time per 50 cycle, else, randomly assign requests
		if (count % 50 == 0) {
			for (int i = 0; i < 3; i++) {
				String url = instances[i].getUrl() + ":8080/info/cpu";
				String response = null;
				while (response == null) {
					try {
						response = sendGet(url);
					} catch (Exception e) {
						// Do nothing
					}
				}
				//				try {
				//					response = sendGet(url);
				//				} catch (Exception e) {
				//					e.printStackTrace();
				//				}

				int start = response.indexOf("<body>");
				int end = response.indexOf("</body>");
				String cpuRaw = response.substring(start + 6, end);
				if (cpuRaw == null) {
					cpu[i] = 0.0;
				} else {
					String temp = cpuRaw.trim();
					if (temp == "" || temp.length() == 0) {
						cpu[i] = 0.0;
					} else {
						cpu[i] = Double.parseDouble(temp);
					}
				}
				//				System.out.println(cpu[i]);
			}
			minIndex = getMinOfThree(cpu[0], cpu[1], cpu[2]);
		} else {
			int factor = random.nextInt(2);
			if (factor == 0) minIndex = (minIndex + 2) % 3;
			if (factor == 1) minIndex = (minIndex + 1) % 3;
		}
		return minIndex;
	}

	// get the min index of three double
	private int getMinOfThree(double d0, double d1, double d2) {
		double temp = Math.min(d0, d1);
		double min = Math.min(temp, d2);
		if (min == d0) return 0;
		if (min == d1) return 1;
		else return 2;
	}

	private int getResponseCode(String url) throws IOException {

		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();

		con.setRequestMethod("GET");
		con.setConnectTimeout(10000);
		con.setReadTimeout(10000);
		
		return con.getResponseCode();
	}

	/* Methods from previous projects */
	
	private BasicAWSCredentials loadProperties() throws IOException {
		// Load the Properties File with AWS Credentials
		Properties properties = new Properties();
		properties.load(LoadBalancer.class
				.getResourceAsStream("/AwsCredentials.properties"));
		BasicAWSCredentials bawsc = new BasicAWSCredentials(
				properties.getProperty("accessKey"),
				properties.getProperty("secretKey"));
		return bawsc;
	}

	private void createTag(AmazonEC2Client ec2, Instance instance) {
		// Create Tags
		Tag tag = new Tag("Project", "2.3");
		CreateTagsRequest createTagsRequest = new CreateTagsRequest();
		createTagsRequest.withResources(instance.getInstanceId()).withTags(tag);
		ec2.createTags(createTagsRequest);
	}

	private RunInstancesRequest createDataCenterRequest() {
		// Create Data Center Request
		RunInstancesRequest dataCenterRequest = new RunInstancesRequest();
		// Configure Data Center Request
		dataCenterRequest.withImageId("ami-ed80c388")
		.withInstanceType("m3.medium").withMinCount(1).withMaxCount(1)
		.withKeyName("15619_Project0")
		.withSecurityGroups("all-traffic-group");
		return dataCenterRequest;
	}

	// HTTP GET request and return page content
	private String sendGet(String url) throws IOException {

		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();

		con.setRequestMethod("GET");
//		System.out.println("\nSending 'GET' request to URL : " + url);

		con.setConnectTimeout(1000);
		con.setReadTimeout(1000);
		BufferedReader in = new BufferedReader(new InputStreamReader(
				con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();

		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();

		// print result
//		System.out.println(response.toString());

		return response.toString();
	}

	private List<Instance> listInstance(AmazonEC2Client ec2) {
		List<Instance> testInstances = new ArrayList<Instance>();
		List<Reservation> reservations = ec2.describeInstances()
				.getReservations();
		int reservationCount = reservations.size();
		for (int i = 0; i < reservationCount; i++) {
			List<Instance> instances = reservations.get(i).getInstances();
			int instanceCount = instances.size();
			// Print the instance IDs of every instance in the reservation.
			for (int j = 0; j < instanceCount; j++) {
				Instance instance = instances.get(j);
				if (instance.getState().getName().equals("running")) {
					testInstances.add(instance);
				}
			}
		}
		return testInstances;
	}
}
