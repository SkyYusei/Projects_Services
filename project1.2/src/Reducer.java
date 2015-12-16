import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

public class Reducer {
	private static final int FIRST_DATE = 20150801;
	
	private static final int MAX_VIEW_NUM = 100000;
	
	/**
	 * Main method to reduce the file
	 */
	public static void main(String[] args) {
		try {
			read();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Helper method to read the stdin
	 * @throws IOException IOException when reading stdin
	 */
	private static void read()
			throws IOException {
		BufferedReader bf = new BufferedReader(new InputStreamReader(System.in));

		String line;
		String word = null;
		String currentWord = null;
		// denote 31 days of view counts
		int[] numOfViews = new int[31];

		// start executing the file
		while ((line = bf.readLine()) != null) {
			String[] words = line.split("\t");	
			// the article name of the line
			word = words[0];
			String[] temp = words[1].split(",");
			// the date of the line
			String date = temp[0];
			// the view count of the line
			int count = Integer.parseInt(temp[1]);
			
			// if the same name was encountered again
			if (currentWord != null && currentWord.equals(word)) {
				
				int index = Integer.parseInt(date) - FIRST_DATE;
				numOfViews[index] += count;
			} else {
				
				// it is not the first word
				if (currentWord != null) {
					int sum = 0;
					// calculate the sum
					for (int j = 0; j < 31; j++) {
						sum += numOfViews[j];
					}
					if (sum > MAX_VIEW_NUM) {
						// build the stdout
						StringBuilder sb = new StringBuilder();
						sb.append(sum);
						sb.append("\t");
						sb.append(currentWord);
						for (int k = 0; k < 31; k++) {
							sb.append("\t");
							sb.append(FIRST_DATE + k);
							sb.append(":");
							sb.append(numOfViews[k]);
							
						}
						System.out.println(sb.toString());
					}
					// we see a new word, first reset the numOfViews
					Arrays.fill(numOfViews, 0);
					// update the count when first seeing the word
					int index = Integer.parseInt(date) - FIRST_DATE;
					numOfViews[index] += count;
					// update the current word to the new one
					currentWord = word;
				} else {
					// update the count when first seeing the word
					int index = Integer.parseInt(date) - FIRST_DATE;
					numOfViews[index] += count;
					// update the current word to the new one
					currentWord = word;
				}
			}
			
		}
		
		// deal with the last word, same as above
		if (currentWord != null && currentWord.equals(word)) {
			int sum = 0;
			for (int j = 0; j < 31; j++) {
				sum += numOfViews[j];
			}
			if (sum > MAX_VIEW_NUM) {
				StringBuilder sb = new StringBuilder();
				sb.append(sum);
				sb.append("\t");
				sb.append(currentWord);
				for (int k = 0; k < 31; k++) {
					sb.append("\t");
					sb.append(FIRST_DATE + k);
					sb.append(":");
					sb.append(numOfViews[k]);
					
				}
				System.out.println(sb.toString());
			}
		}
		// close the reader at the end
		bf.close();
	}
}
