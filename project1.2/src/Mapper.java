import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Mapper {
	/*
	 * Some constant variables given by the instruction
	 */
	private static final String[] EXCLUDE_PAGE = { "Media:", "Special:",
			"Talk:", "User:", "User_talk:", "Project:", "Project_talk:",
			"File:", "File_talk:", "MediaWiki:", "MediaWiki_talk:",
			"Template:", "Template_talk:", "Help:", "Help_talk:", "Category:",
			"Category_talk:", "Portal:", "Wikipedia:", "Wikipedia_talk:" };

	private static final String[] EXCLUDE_IMG = { ".jpg", ".gif", ".png",
			".JPG", ".GIF", ".PNG", ".txt", ".ico" };

	private static final String[] EXCLUDE_BOIL = { "404_error/", "Main_Page",
			"Hypertext_Transfer_Protocol", "Search" };
	
	private static final String GIVEN_DATE_PREFIX = "201508";
	
	/**
	 * Main method to map the file
	 */
	public static void main(String[] args) {
		try {
			read();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Helper method to read stdin
	 * @throws IOException IOException when reading stdin
	 */
	private static void read()
			throws IOException {
		// get the fullFilename
		String fullFilename = System.getenv("mapreduce_map_input_file");
		String date = null;
		try {
			int index = fullFilename.lastIndexOf("/");
			String filename = fullFilename.substring(index+1);
			date = filename.split("-")[1];
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (date == null || !date.startsWith(GIVEN_DATE_PREFIX))
			throw new IllegalArgumentException("File name error");
		
		BufferedReader bf = new BufferedReader(new InputStreamReader(System.in));

		String line;
		while ((line = bf.readLine()) != null) {
			String[] words = line.split("\\s+");
			// filter out all items that are not of 4 columns, and start with
			// "en"
			if (words.length == 4 && words[0].equals("en")) {
				boolean exFlag = false; // find if the item should be excluded

				// filter out lower cases
				if (words[1].charAt(0) >= 97 && words[1].charAt(0) <= 122) {
					continue;
				}

				// filter out excluded pages
				for (String ep : EXCLUDE_PAGE) {
					if (words[1].startsWith(ep)) {
						exFlag = true;
						break;
					}
				}

				if (exFlag) continue;
				
				// filter out excluded images
				for (String ei : EXCLUDE_IMG) {
					if (words[1].endsWith(ei)) {
						exFlag = true;
						break;
					}
				}

				if (exFlag) continue;
				
				// filter out excluded boilerplate pages
				for (String eb : EXCLUDE_BOIL) {
					if (words[1].equals(eb)) {
						exFlag = true;
						break;
					}
				}
				
				if (exFlag) continue;
				
				// if the item does not contain any exclude flags
				System.out.println(words[1] + "\t" + date + "," + words[2]);
			}
		}
		// close the reader at the end
		bf.close();
	}
}
