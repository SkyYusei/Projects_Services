import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.lang3.StringEscapeUtils;


public class DatabaseInitialization {
	private static EntryDAO entryDAO;
	
	public static void main(String[] args) {		
		entryDAO = new EntryDAO();
//		String inputFile = "part-00000";
		try {
//			BufferedReader bf = new BufferedReader(new InputStreamReader(
//                    new FileInputStream(inputFile), "UTF8"));
			BufferedReader bf = new BufferedReader(new InputStreamReader(System.in, "UTF8"));
			String line;
			while ((line = bf.readLine()) != null) {				
				processLine(line);
			}
			bf.close();
			System.out.println("**************************");
			System.out.println("Success building database.");
			System.out.println("**************************");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static void processLine(String line) {
		String userId, tweetTime, tweetId, score, tweetText;
		String words[] = line.split("\t");
		try {
			userId = words[0];
			tweetTime = words[1];
			tweetId = words[2];
			score = words[3];
			tweetText = words[4];
			tweetText = StringEscapeUtils.escapeJava(tweetText);
		} catch (Exception e) {
			System.err.println("Error when building databse, line format error");
			return;
		}
		Integer i = EntryDAO.hashFunc(userId, tweetTime);
		try {			
			entryDAO.addEntry(i, userId, tweetTime, tweetId, score, tweetText);
		}  catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
