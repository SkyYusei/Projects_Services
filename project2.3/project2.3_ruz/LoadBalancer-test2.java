import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URL;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LoadBalancer {
	private static final int THREAD_POOL_SIZE = 4;
	private final ServerSocket socket;
	private final DataCenterInstance[] instances;

	public LoadBalancer(ServerSocket socket, DataCenterInstance[] instances) {
		this.socket = socket;
		this.instances = instances;
	}

	// Complete this function
	public void start() throws IOException {
		ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
		Double[] cpu = new Double[3];
		int count = 0, minIndex = 0;
		Random random = new Random();
		while (true) {
			if (count % 10 == 0) {
				for (int i = 0; i < 3; i++) {
					String url = instances[i].getUrl() + ":8080/info/cpu";
					String response = null;
//					while (true) {
//						try {
//							response = sendGet(url);
//							if (response != null) break;
//						} catch (Exception e) {
//							// Do nothing
//						}
//					}
					try {
						response = sendGet(url);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

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
//					System.out.println(cpu[i]);
				}
				minIndex = getMinIndex(cpu[0], cpu[1], cpu[2]);
			} else {
				int factor = random.nextInt(2);
				if (factor == 0) minIndex = (minIndex + 2) % 3;
				if (factor == 1) minIndex = (minIndex + 1) % 3;
			}
			count++;
			
			
			Runnable requestHandler = new RequestHandler(socket.accept(), instances[count]);
			
			executorService.execute(requestHandler);
		}
	}

	// HTTP GET request and return page content
	private static String sendGet(String url) throws Exception {

		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();

		con.setRequestMethod("GET");
//		System.out.println("\nSending 'GET' request to URL : " + url);

		con.setReadTimeout(20000);
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
	
	private int getMinIndex(double d0, double d1, double d2) {
		double temp = Math.min(d0, d1);
		double min = Math.min(temp, d2);
		if (min == d0) return 0;
		if (min == d1) return 1;
		else return 2;
	}
}
