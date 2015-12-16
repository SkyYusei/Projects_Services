import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class filter {
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

	/**
	 * Main method to filter the file
	 * @param args args[0] is the file name parsed in
	 */
	public static void main(String[] args) {
		// args[0] is the filename to parse in
		if (args.length != 1)
			throw new IllegalArgumentException(
					"the only one arguemnt should be the filename");

		String outputFile = "output";
		try {
			readFile(args[0], outputFile);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Helper method to read the file
	 * @param inputFile the input filename
	 * @param outputFile the output filename
	 * @throws IOException IOException when reading file
	 */
	private static void readFile(String inputFile, String outputFile)
			throws IOException {
		BufferedReader bf = new BufferedReader(new FileReader(inputFile));
		BufferedWriter out = new BufferedWriter(new FileWriter(outputFile));

		String line;
		while ((line = bf.readLine()) != null) {
			// write to a file
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
				out.write(words[1] + "\t" + words[2] + "\n");
			}
		}
		// close the reader and writer at the end
		bf.close();
		out.close();
	}
}
