import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


public class Q6 {
	public static void main(String[] args) throws IOException {
		// read output from stdin
		BufferedReader bf = new BufferedReader(new InputStreamReader(System.in));
		// read q6 file
		BufferedReader bf2 = new BufferedReader(new FileReader("q6"));
		
		// map tp store filmname and max view
		Map<String, Integer> map = new HashMap<String, Integer>();
		// list to store the film names
		List<String> filmname = new ArrayList<String>();
		
		// read film name file
		String name;
		while ((name = bf2.readLine()) != null) {
			filmname.add(name);
		}
		
		// process the input
		String line;
		while ((line = bf.readLine()) != null) {
			String[] words = line.split("\t");
			for (String fn : filmname) {
				// found the name
				if (words[1].equals(fn)) {
					int maxView = 0, currentView;
					// find the max view
					for (int i = 0; i < 31; i++) {
						currentView = Integer.parseInt(words[i+2].split(":")[1]);
						if (currentView > maxView)
							maxView = currentView;
					}
					map.put(fn, maxView);
				}
			}
			// if we process all the names, we do not need to read the input
			if (map.size() >= filmname.size())
				break;
		}
				
		bf.close();
		bf2.close();
		
		Map<String, Integer> sortedMap = sortByComparator(map);
		printMap(sortedMap);
	}
	
	/**
	 * helper method to sort the map by value
	 * @param unsortMap the orinal map parsed in
	 * @return the sorted map
	 */
	private static Map<String, Integer> sortByComparator(Map<String, Integer> unsortMap) {

		// Convert Map to List
		List<Map.Entry<String, Integer>> list = 
			new LinkedList<Map.Entry<String, Integer>>(unsortMap.entrySet());

		// Sort list with comparator, to compare the Map values
		Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
			public int compare(Map.Entry<String, Integer> o1,
                                           Map.Entry<String, Integer> o2) {
				return -(o1.getValue()).compareTo(o2.getValue());
			}
		});

		// Convert sorted map back to a Map
		Map<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();
		for (Iterator<Map.Entry<String, Integer>> it = list.iterator(); it.hasNext();) {
			Map.Entry<String, Integer> entry = it.next();
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}
	
	/**
	 * helper method to print the names separated by comma
	 * @param map original map parsed in
	 */
	private static void printMap(Map<String, Integer> map) {
		// we need to translate the map to a list
		List<String> out = new ArrayList<String>();
		for (Map.Entry<String, Integer> entry : map.entrySet()) {
			out.add(entry.getKey());
		}
		for (int i = 0; i < out.size() - 1;i++) {
			System.out.print(out.get(i) + ",");
		}
		System.out.print(out.get(out.size() - 1));
		System.out.print("\n");
	}
}
