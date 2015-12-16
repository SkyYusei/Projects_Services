import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


public class Q5 {
	public static void main(String[] args) throws IOException {
		BufferedReader bf = new BufferedReader(new InputStreamReader(System.in));
		
		// Store the facebook data
		int[] fb = new int[31];
		// Store the twitter data
		int[] tw = new int[31];
		// flag to control if the two lines are found, then we do not need to read again
		boolean fbFound = false;
		boolean twFound = false;
		
		String line;
		while ((line = bf.readLine()) != null) {
			String[] words = line.split("\t");
			// found Facebook
			if (words[1].equals(args[0])) {
				for (int i = 0; i < 31; i++) {
					fb[i] = Integer.parseInt(words[i+2].split(":")[1]);
				}
				fbFound = true;
			}
			// Found Twitter
			if (words[1].equals(args[1])) {
				for (int j = 0; j < 31; j++) {
					tw[j] = Integer.parseInt(words[j+2].split(":")[1]);
				}
				twFound = true;
			}
			if (fbFound && twFound)
				break;
		}
		
		// calculate the days
		int numOfDays = 0;
		for (int k = 0; k < 31; k++) {
			if (fb[k] > tw[k])
				numOfDays++;
		}
		
		System.out.println(numOfDays);
	}
}
