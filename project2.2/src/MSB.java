import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.autoscaling.AmazonAutoScalingClient;
import com.amazonaws.services.autoscaling.model.CreateAutoScalingGroupRequest;
import com.amazonaws.services.autoscaling.model.CreateLaunchConfigurationRequest;
import com.amazonaws.services.autoscaling.model.DeleteAutoScalingGroupRequest;
import com.amazonaws.services.autoscaling.model.DeleteLaunchConfigurationRequest;
import com.amazonaws.services.autoscaling.model.PutScalingPolicyRequest;
import com.amazonaws.services.autoscaling.model.PutScalingPolicyResult;
import com.amazonaws.services.autoscaling.model.UpdateAutoScalingGroupRequest;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.ComparisonOperator;
import com.amazonaws.services.cloudwatch.model.PutMetricAlarmRequest;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.cloudwatch.model.Statistic;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.CreateSecurityGroupRequest;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.CreateVolumeRequest;
import com.amazonaws.services.ec2.model.IpPermission;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancingClient;
import com.amazonaws.services.elasticloadbalancing.model.ConfigureHealthCheckRequest;
import com.amazonaws.services.elasticloadbalancing.model.CreateLoadBalancerRequest;
import com.amazonaws.services.elasticloadbalancing.model.CreateLoadBalancerResult;
import com.amazonaws.services.elasticloadbalancing.model.DescribeInstanceHealthRequest;
import com.amazonaws.services.elasticloadbalancing.model.DescribeInstanceHealthResult;
import com.amazonaws.services.elasticloadbalancing.model.HealthCheck;
import com.amazonaws.services.elasticloadbalancing.model.InstanceState;
import com.amazonaws.services.elasticloadbalancing.model.Listener;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.Instance;

public class MSB {
	private static final String KEY = "Project";
	private static final String VALUE = "2.2";
	private static final int VOLUME_SIZE = 30;
	private static final String AVAILABLILITY_ZONE = "us-east-1b";
	private static final String SUBMISSION_PASSWORD = "AUAJwrkEH6kEkRpsxLJWDzIerdyagjUw";
	private static final int WAIT_CYCLE = 1000;
	private static Instance loadGen;

	public static void main(String[] args) throws Exception {
		// Load the Properties File with AWS Credentials
		BasicAWSCredentials bawsc = loadProperties();

		// Create an Amazon EC2 Client
		AmazonEC2Client ec2 = new AmazonEC2Client(bawsc);

		// Create ELB Client
		AmazonElasticLoadBalancingClient elb = new AmazonElasticLoadBalancingClient(bawsc);
		
		// Create AutoScaling Client
		AmazonAutoScalingClient asg = new AmazonAutoScalingClient(bawsc);
		
		// Create CloudWatch Client
		AmazonCloudWatchClient cloudWatchClient = new AmazonCloudWatchClient();

		/* Start LG */
		
		// Create Volume Request
//		createVolumeRequest(ec2);

		// Create Security to allow all traffic
//		createSecurityGroup(ec2);

		// Create Instance Request
		RunInstancesRequest loadGenRequest = createLoadGenRequest();

		// Launch Instance
		RunInstancesResult runLoadGenResult = ec2.runInstances(loadGenRequest);

		// Return the Load Generator
		loadGen = runLoadGenResult.getReservation().getInstances().get(0);

		String loadGenId = loadGen.getInstanceId();
		
		// Tag the loadGen
		Tag tag = new Tag(KEY, VALUE);
		CreateTagsRequest createTagsRequest = new CreateTagsRequest();
		createTagsRequest.withResources(loadGen.getInstanceId()).withTags(tag);
		ec2.createTags(createTagsRequest);
		
		// check if running and then set tag assign loadGen to new found instances
		while (!checkInstanceRunning(ec2, loadGenId)) {
			try {
				Thread.sleep(WAIT_CYCLE * 5);
			} catch (InterruptedException e) {
				// Do Nothing
			}
		}

		Thread.sleep(WAIT_CYCLE * 200);

		// activate load instance
		sendGet("http://" + loadGen.getPublicDnsName());
		
		/* End LG, Start ELB */
		System.out.println("\nEnd LG, Start ELB\n");
		
		// create load balancer request
		CreateLoadBalancerRequest lbRequest = createLoadBanlancerRequest();

		// Create Health Check Request
		ConfigureHealthCheckRequest healthCheckRequest = createHealthCheckRequest(loadGen.getPublicDnsName());

		// create load balancer
		CreateLoadBalancerResult lbResult = elb.createLoadBalancer(lbRequest);
		
		// configure load balancer
		elb.configureHealthCheck(healthCheckRequest);
		
		// get elb dns
		String elbdns = lbResult.getDNSName();

		/* End ELB, Start ASG */
		System.out.println("\nEnd ELB, Start ASG\n");
		
		// Launch configuration
		CreateLaunchConfigurationRequest launchConfRequest = createLaunchConfigurationRequest();

		// get Monitoring
		launchConfRequest.getInstanceMonitoring();
		asg.createLaunchConfiguration(launchConfRequest);

		// Auto scaling group request
		CreateAutoScalingGroupRequest asgRequest = createAutoScalingGroupRequest();

		// Create auto scaling group
		asg.createAutoScalingGroup(asgRequest);

		// Set auto scaling up policy
		PutScalingPolicyRequest scaleUp = createScalingUpPolicyRequest();
		PutScalingPolicyResult ScaleUpResult = asg.putScalingPolicy(scaleUp);
		String arnScaleUp = ScaleUpResult.getPolicyARN();
		setScaleUpPolicy(arnScaleUp, cloudWatchClient);
		
		// Set auto scaling down policy
		PutScalingPolicyRequest scaleDown = createScalingDownPolicyRequest();
		PutScalingPolicyResult ScaleDownResult = asg.putScalingPolicy(scaleDown);
		String arnScaleDown = ScaleDownResult.getPolicyARN();
		setScaleDownPolicy(arnScaleDown, cloudWatchClient);

		System.out.println("\nStart health check\n");
		// wait for health check
		boolean flag = true;
		while (flag) {
			DescribeInstanceHealthResult health = elb
					.describeInstanceHealth(new DescribeInstanceHealthRequest()
							.withLoadBalancerName("Project2point2ELB"));
			
			List<InstanceState> instanceStates = health.getInstanceStates();

			for (int i = 0; i < instanceStates.size(); i++) {
				InstanceState temp = instanceStates.get(i);
				if (temp.getState().equals("InService")) {
					System.out.println("Instance in service");
					flag = false;
					break;
				}
			}
			Thread.sleep(WAIT_CYCLE * 60);
		}
		
		System.out.println("\nEntering password\n");
		String passwordUrl = "http://" + loadGen.getPublicDnsName() + 
				"/password?passwd=" + SUBMISSION_PASSWORD;
		sendGet(passwordUrl);
		
		System.out.println("\nBegin warm up\n");
		// warm up
		warmupUp(loadGen.getPublicDnsName(), elbdns);

		System.out.println("\nStart test\n");
		// start test
		String startTesturl = "http://" + loadGen.getPublicDnsName() + 
				"/junior?dns=" + elbdns;
		sendGet(startTesturl);
		
		// run for more than 48min
		Thread.sleep(WAIT_CYCLE * 50 * 60);
		
		delete(asg);

	}
	
	private static BasicAWSCredentials loadProperties() throws IOException {
		// Load the Properties File with AWS Credentials
		Properties properties = new Properties();
		properties.load(MSB.class
				.getResourceAsStream("/AwsCredentials.properties"));
		BasicAWSCredentials bawsc = new BasicAWSCredentials(
				properties.getProperty("accessKey"),
				properties.getProperty("secretKey"));
		return bawsc;
	}
	
	private static void createVolumeRequest(AmazonEC2Client ec2) {
		// Create Tags
		CreateVolumeRequest createVolumeRequest = new CreateVolumeRequest(
				VOLUME_SIZE, AVAILABLILITY_ZONE);
		ec2.createVolume(createVolumeRequest);
	}

	private static void createSecurityGroup(AmazonEC2Client ec2) {
		try {
			CreateSecurityGroupRequest securityGroupRequest = new CreateSecurityGroupRequest(
					"allTraffic", "Group With All traffic");
			ec2.createSecurityGroup(securityGroupRequest);
		} catch (AmazonServiceException ase) {

		}

		IpPermission ipPermission = new IpPermission().withIpProtocol("-1")
				.withFromPort(new Integer(-1)).withToPort(new Integer(-1))
				.withIpRanges("0.0.0.0/0");
		AuthorizeSecurityGroupIngressRequest authorizeSecurityGroupIngressRequest = new AuthorizeSecurityGroupIngressRequest();

		authorizeSecurityGroupIngressRequest.withGroupName("allTraffic")
				.withIpPermissions(ipPermission);

		ec2.authorizeSecurityGroupIngress(authorizeSecurityGroupIngressRequest);
	}
	
	private static Collection<Tag> createTag() {
		// Create Tags
		Collection<Tag> tag = new ArrayList<Tag>();
		Tag tagValue = new Tag();
		tagValue.setKey(KEY);
		tagValue.setValue(VALUE);
		tag.add(tagValue);
		return tag;
	}
	
	private static RunInstancesRequest createLoadGenRequest() {
		// Create Load Generator Request
		RunInstancesRequest loadGenRequest = new RunInstancesRequest();
		// Configure Load Generator Request
		loadGenRequest.withImageId("ami-312b5154")
		.withInstanceType("m3.medium")
		.withMinCount(1)
		.withMaxCount(1)
		.withKeyName("15619_Project0")
		.withSecurityGroups("all-traffic-group");
		return loadGenRequest;
	}

	private static CreateLoadBalancerRequest createLoadBanlancerRequest() {
		CreateLoadBalancerRequest clbRequest = new CreateLoadBalancerRequest();
		com.amazonaws.services.elasticloadbalancing.model.Tag tag = 
				new com.amazonaws.services.elasticloadbalancing.model.Tag();
		tag.withKey(KEY).withValue(VALUE);
		
		List<Listener> listeners = new ArrayList<Listener>(1);
		listeners.add(new Listener("HTTP", 80, 80));
		
		clbRequest.withLoadBalancerName("Project2point2ELB").withListeners(listeners)
				.withTags(tag)
				.withAvailabilityZones(AVAILABLILITY_ZONE);
		
		return clbRequest;
	}
	
	private static ConfigureHealthCheckRequest createHealthCheckRequest(String lgDnsName) {
		HealthCheck hc = new HealthCheck();
		hc.withTarget("HTTP:80/heartbeat?lg=" + lgDnsName)
			.withInterval(30).withTimeout(5).withHealthyThreshold(10)
			.withUnhealthyThreshold(2);

		ConfigureHealthCheckRequest healthCheckRequest = new ConfigureHealthCheckRequest();
		healthCheckRequest.withHealthCheck(hc).withLoadBalancerName("Project2point2ELB");
		
		return healthCheckRequest;
	}

	private static CreateLaunchConfigurationRequest createLaunchConfigurationRequest() {
		CreateLaunchConfigurationRequest launchConfRequest = new CreateLaunchConfigurationRequest();
		launchConfRequest.withImageId("ami-3b2b515e").withInstanceType("m3.medium")
			.withLaunchConfigurationName("LaunchConfigurationProject2point2")
			.withSecurityGroups("all-traffic-group");
		return launchConfRequest;
	}

	private static CreateAutoScalingGroupRequest createAutoScalingGroupRequest() {
		CreateAutoScalingGroupRequest asgRequest = new CreateAutoScalingGroupRequest();
		com.amazonaws.services.autoscaling.model.Tag tag = 
				new com.amazonaws.services.autoscaling.model.Tag();
		tag.withKey(KEY).withValue(VALUE);
		asgRequest.withAvailabilityZones(AVAILABLILITY_ZONE)
			.withLoadBalancerNames("Project2point2ELB")
			.withMinSize(4)
			.withMaxSize(4)
			.withDesiredCapacity(4)
			.withAutoScalingGroupName("AutoScalingGroupProject2point2")
			.withLaunchConfigurationName("LaunchConfigurationProject2point2")
			.withTags(tag);
		return asgRequest;
	}
	
	// check if the instance is running
	private static boolean checkInstanceRunning(AmazonEC2Client ec2, String instanceid) {
		Collection<Tag> tag = createTag();
		List<Reservation> reservations = ec2.describeInstances().getReservations();
		int reservationSize = reservations.size();
		for (int i = 0; i < reservationSize; i++) {
			List<Instance> instances = reservations.get(i).getInstances();
			int instanceSize = instances.size();
			for (int j = 0; j < instanceSize; j++) {
				Instance instance = instances.get(j);
				if (instance.getInstanceId().equals(instanceid)) {
					if (instance.getState().getName().equals("running")) {
						loadGen = instance;
						loadGen.setTags(tag);
						return true;
					}
				}
			}
		}
		return false;
	}

	private static PutScalingPolicyRequest createScalingUpPolicyRequest() {
		PutScalingPolicyRequest up = new PutScalingPolicyRequest();
		up.withAutoScalingGroupName("AutoScalingGroupProject2point2")
		.withScalingAdjustment(1)
		.withAdjustmentType("ChangeInCapacity")
		.withPolicyName("Scale Up").withCooldown(60);
		return up;
	}
	
	private static PutScalingPolicyRequest createScalingDownPolicyRequest() {
		PutScalingPolicyRequest down = new PutScalingPolicyRequest();
		down.withAutoScalingGroupName("AutoScalingGroupProject2point2")
		.withScalingAdjustment(-1)
		.withAdjustmentType("ChangeInCapacity")
		.withPolicyName("Scale Down").withCooldown(60);
		return down;
	}
	
	// Set up scale policy
	private static void setScaleUpPolicy(String arn, AmazonCloudWatchClient cloudWatchClient) {
		PutMetricAlarmRequest request = new PutMetricAlarmRequest();
		request.setAlarmName("AlarmUp");

		request.setMetricName("CPUUtilization");

		request.setNamespace("AWS/EC2");
		request.setComparisonOperator(ComparisonOperator.GreaterThanThreshold);

		request.setStatistic(Statistic.Average);
		request.setUnit(StandardUnit.Percent);
		request.setEvaluationPeriods(1);
		request.setThreshold(70.0);
		request.setPeriod(60);

		List<String> actions = new ArrayList<String>();
		actions.add(arn);
		request.setAlarmActions(actions);

		cloudWatchClient.putMetricAlarm(request);
	}
	
	private static void setScaleDownPolicy(String arn, AmazonCloudWatchClient cloudWatchClient) {
		PutMetricAlarmRequest request = new PutMetricAlarmRequest();

		request.setAlarmName("AlarmDown");
		request.setMetricName("CPUUtilization");

		request.setNamespace("AWS/EC2");

		request.setComparisonOperator(ComparisonOperator.LessThanThreshold);

		request.setStatistic(Statistic.Average);
		request.setUnit(StandardUnit.Percent);
		request.setEvaluationPeriods(1);

		request.setThreshold(35.0);
		request.setPeriod(60);


		List<String> actions = new ArrayList<String>();
		actions.add(arn); // This is the value returned by the ScalingPolicy
		// request
		request.setAlarmActions(actions);

		cloudWatchClient.putMetricAlarm(request);
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

	private static void warmupUp(String loader, String elbdns)
			throws Exception {
		String url = "http://" + loader + "/warmup?dns=" + elbdns;
		
		System.out.println("Warm up start!");

		// Do warm up eight times
		for (int i = 0; i < 8; i++) {
			sendGet(url);
			// warm up for 6min which is over 5 times
			Thread.sleep(WAIT_CYCLE * 6 * 60);

		}

	}
	
	private static void delete(AmazonAutoScalingClient asg) throws InterruptedException{
		UpdateAutoScalingGroupRequest asgRequest = new UpdateAutoScalingGroupRequest();
	
		asgRequest.withAutoScalingGroupName("autoScalingGroup")
		.withMaxSize(0)
		.withMinSize(0)
		.withDesiredCapacity(0);
		
		asg.updateAutoScalingGroup(asgRequest);
		Thread.sleep(WAIT_CYCLE * 200);
		
		DeleteAutoScalingGroupRequest dasgRequest = new DeleteAutoScalingGroupRequest();
		dasgRequest.withAutoScalingGroupName("autoScalingGroup");
		asg.deleteAutoScalingGroup(dasgRequest);
		
		DeleteLaunchConfigurationRequest dlcRequest = new DeleteLaunchConfigurationRequest();
		dlcRequest.setLaunchConfigurationName("launchConfiguration");
		asg.deleteLaunchConfiguration(dlcRequest);
	}

}
