import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Q8 {
	// title we need to find
	private static final String NAME = "NASDAQ-100";
	// first date
	private static final int GIVEN_DATE_PREFIX = 20150801;
	
	public static void main(String[] args) throws IOException {
		// read output from stdin
		BufferedReader bf = new BufferedReader(new InputStreamReader(System.in));

		// process the input
		String line;
		while ((line = bf.readLine()) != null) {
			String[] words = line.split("\t");
			// found NAME
			if (words[1].equals(NAME)) {
				// find the max view
				int maxView = 0, currentView, index = 0;
				for (int i = 0; i < 31; i++) {
					currentView = Integer.parseInt(words[i+2].split(":")[1]);
					if (currentView > maxView) {
						maxView = currentView;
						index = i;
					}
				}
				// print out the date and then exit
				System.out.println(GIVEN_DATE_PREFIX + index);
				break;
			}
		}

		bf.close();

	}
}
