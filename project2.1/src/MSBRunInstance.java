import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Stack;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.CreateSecurityGroupRequest;
import com.amazonaws.services.ec2.model.CreateSecurityGroupResult;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.CreateVolumeRequest;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.IpPermission;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.Tag;

public class MSBRunInstance {
	private static final String KEY = "Project";
	private static final String VALUE = "2.1";
	private static final int VOLUME_SIZE = 30;
	private static final String AVAILABLILITY_ZONE = "us-east-1d";
	private static final String SUBMISSION_PASSWORD = "AUAJwrkEH6kEkRpsxLJWDzIerdyagjUw";
	private static final int SLEEP_CYCLE = 100000;
	private static final int WAIT_CYCLE = 1000;

	public static void main(String[] args) throws Exception {
		BasicAWSCredentials bawsc = loadProperties();

		// Create an Amazon EC2 Client
		AmazonEC2Client ec2 = new AmazonEC2Client(bawsc);

		// Create Volume Request
		createVolumeRequest(ec2);
		
		// Create Security to allow all traffic
		CreateSecurityGroup(ec2);

		// Create Load Generator Request
		RunInstancesRequest loadGenRequest = createLoadGenRequest();

		// Create Data Center Request
		RunInstancesRequest dataCenterRequest = createDataCenterRequest();

		// Launch Instance
		RunInstancesResult runLoadGenResult = ec2.runInstances(loadGenRequest);
		RunInstancesResult runDataCenterResult = ec2
				.runInstances(dataCenterRequest);

		// Return the Object Reference of the Instance just Launched
		Instance loadGenInstance = runLoadGenResult.getReservation()
				.getInstances().get(0);
		Instance dataCenterInstance = runDataCenterResult.getReservation()
				.getInstances().get(0);

		String loadGenId = loadGenInstance.getInstanceId();
		String dataCenterId = dataCenterInstance.getInstanceId();

		// Create Tags
		createTag(ec2, loadGenInstance);
		createTag(ec2, dataCenterInstance);

		System.out
				.println("Waiting for load generator and first data center to be running");

		while (listInstance(ec2).size() != 2) {
			try {
				Thread.sleep(WAIT_CYCLE);
			} catch (InterruptedException e) {
				// Do Nothing
			}
		}

		// assign the two instances according to instance id
		List<Instance> testInstances = listInstance(ec2);
		if (testInstances.get(0).getInstanceId().equals(loadGenId)) {
			loadGenInstance = testInstances.get(0);
			dataCenterInstance = testInstances.get(1);
		} else if (testInstances.get(0).getInstanceId().equals(dataCenterId)) {
			dataCenterInstance = testInstances.get(0);
			loadGenInstance = testInstances.get(1);
		}

		Thread.sleep(SLEEP_CYCLE);
		System.out.println("Load generator and first data center are running");

		// Submit password to the load generator
		String submitPasswordUrl = "http://"
				+ loadGenInstance.getPublicDnsName() + "/password?passwd="
				+ SUBMISSION_PASSWORD;
		sendGet(submitPasswordUrl);

		// Submit the data center DNS name to the load generator to start the
		// test
		String startTestUrl = "http://" + loadGenInstance.getPublicDnsName()
				+ "/test/horizontal?dns="
				+ dataCenterInstance.getPublicDnsName();
		String testNumSrc = sendGet(startTestUrl);

		// The log number is between test. and .log
		int start = testNumSrc.indexOf("test.");
		int end = testNumSrc.indexOf(".log");
		String testNum = testNumSrc.substring(start + 5, end);

		double rps = 0;
		while (rps < 4000) {
			// Run and get the log
			String getLogUrl = "http://" + loadGenInstance.getPublicDnsName()
					+ "/log?name=test." + testNum + ".log";
			rps = sendGetLastLines(getLogUrl);

			System.out.println("current rps: " + rps);

			// Add a new data center instance
			RunInstancesRequest addDataCenterRequest = createDataCenterRequest();
			RunInstancesResult addDataCenterResult = ec2
					.runInstances(addDataCenterRequest);
			Instance addedDataCenterInstance = addDataCenterResult
					.getReservation().getInstances().get(0);
			String addedDataCenterId = addedDataCenterInstance.getInstanceId();
			createTag(ec2, addedDataCenterInstance);

			// sleep 100s at this time also wait for instance running
			Thread.sleep(SLEEP_CYCLE);

			// Wait for data center ready
			while (true) {
				System.out.println("Waiting for data center ready");
				String testSentence = null;

				List<Instance> allInstances = listInstance(ec2);
				for (Instance inst : allInstances) {
					if (inst.getInstanceId().equals(addedDataCenterId)) {
						addedDataCenterInstance = inst;
						break;
					}
				}

				// try to load the data center's page to see if dc is ready
				String randomUserUrl = "http://"
						+ addedDataCenterInstance.getPublicDnsName()
						+ "/lookup/random";
				try {
					testSentence = sendGet(randomUserUrl);
				} catch (Exception e) {
					// Do nothing
				}

				if (testSentence != null) {
					break;
				}
				Thread.sleep(WAIT_CYCLE * 2);
			}

			// find the current state of the new added instance
			List<Instance> allInstances = listInstance(ec2);
			for (Instance inst : allInstances) {
				if (inst.getInstanceId().equals(addedDataCenterId)) {
					addedDataCenterInstance = inst;
					break;
				}
			}

			// Add the new instance to test
			String addInstanceUrl = "http://"
					+ loadGenInstance.getPublicDnsName()
					+ "/test/horizontal/add?dns="
					+ addedDataCenterInstance.getPublicDnsName();
			sendGet(addInstanceUrl);

		}

	}

	private static BasicAWSCredentials loadProperties() throws IOException {
		// Load the Properties File with AWS Credentials
		Properties properties = new Properties();
		properties.load(MSBRunInstance.class
				.getResourceAsStream("/AwsCredentials.properties"));
		BasicAWSCredentials bawsc = new BasicAWSCredentials(
				properties.getProperty("accessKey"),
				properties.getProperty("secretKey"));
		return bawsc;
	}

	private static RunInstancesRequest createLoadGenRequest() {
		// Create Load Generator Request
		RunInstancesRequest loadGenRequest = new RunInstancesRequest();
		// Configure Load Generator Request
		loadGenRequest.withImageId("ami-4389fb26")
				.withInstanceType("m3.medium").withMinCount(1).withMaxCount(1)
				.withKeyName("15619_Project0")
				.withSecurityGroups("all-traffic-group");
		return loadGenRequest;
	}

	private static RunInstancesRequest createDataCenterRequest() {
		// Create Data Center Request
		RunInstancesRequest dataCenterRequest = new RunInstancesRequest();
		// Configure Data Center Request
		dataCenterRequest.withImageId("ami-abb8cace")
				.withInstanceType("m3.medium").withMinCount(1).withMaxCount(1)
				.withKeyName("15619_Project0")
				.withSecurityGroups("all-traffic-group");
		return dataCenterRequest;
	}

	private static void createTag(AmazonEC2Client ec2, Instance instance) {
		// Create Tags
		Tag tag = new Tag(KEY, VALUE);
		CreateTagsRequest createTagsRequest = new CreateTagsRequest();
		createTagsRequest.withResources(instance.getInstanceId()).withTags(tag);
		ec2.createTags(createTagsRequest);
	}

	private static void createVolumeRequest(AmazonEC2Client ec2) {
		// Create Tags
		CreateVolumeRequest createVolumeRequest = new CreateVolumeRequest(
				VOLUME_SIZE, AVAILABLILITY_ZONE);
		ec2.createVolume(createVolumeRequest);
	}

	private static void CreateSecurityGroup(AmazonEC2Client ec2) {
		try {
			CreateSecurityGroupRequest securityGroupRequest = new CreateSecurityGroupRequest(
					"allTraffic", "Group With All traffic");
			CreateSecurityGroupResult result = ec2
					.createSecurityGroup(securityGroupRequest);
		} catch (AmazonServiceException ase) {

		}

		IpPermission ipPermission = new IpPermission().withIpProtocol("-1")
				.withFromPort(new Integer(-1)).withToPort(new Integer(-1))
				.withIpRanges("0.0.0.0/0");
		AuthorizeSecurityGroupIngressRequest authorizeSecurityGroupIngressRequest = new AuthorizeSecurityGroupIngressRequest();

		authorizeSecurityGroupIngressRequest.withGroupName("allTrafficGroup")
				.withIpPermissions(ipPermission);

		ec2.authorizeSecurityGroupIngress(authorizeSecurityGroupIngressRequest);
	}
	
	// HTTP GET request and return page content
	private static String sendGet(String url) throws Exception {

		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();

		con.setRequestMethod("GET");
		System.out.println("\nSending 'GET' request to URL : " + url);

		con.setReadTimeout(WAIT_CYCLE * 30);
		BufferedReader in = new BufferedReader(new InputStreamReader(
				con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();

		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();

		// print result
		System.out.println(response.toString());

		return response.toString();
	}

	// HTTP GET request and return the rps from the page content
	private static double sendGetLastLines(String url) throws Exception {

		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();

		con.setRequestMethod("GET");
		System.out.println("\nSending 'GET' request to URL : " + url);

		con.setReadTimeout(WAIT_CYCLE * 30);
		BufferedReader in = new BufferedReader(new InputStreamReader(
				con.getInputStream()));
		String inputLine;
		Stack<String> stack = new Stack<String>();

		while ((inputLine = in.readLine()) != null) {
			// do not want to get the empty line
			if (inputLine.length() > 2) {
				stack.push(inputLine);
			}

		}
		in.close();

		// Using a stack to get last several useful lines
		double rps = 0;
		String line;
		while (!stack.empty() && (line = stack.pop()) != null) {
			try {
				String[] output = line.split("=");
				if (output != null && output[0].endsWith("amazonaws.com")) {
					rps += Double.parseDouble(output[1]);
				} else { // we get a [minute] line
					break;
				}
			} catch (Exception e) {
				break;
			}
		}
		return rps;
	}

	// List all running instances
	private static List<Instance> listInstance(AmazonEC2Client ec2) {
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
