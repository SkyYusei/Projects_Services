import java.io.BufferedReader;
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

public class Q9 {
	
	public static void main(String[] args) throws IOException {
		BufferedReader bf = new BufferedReader(new InputStreamReader(System.in));

		Map<String, Integer> map = new HashMap<String, Integer>();
		
		String line;
		while ((line = bf.readLine()) != null) {
			String[] words = line.split("\t");
			int[] input = new int[31];
			
			for (int i = 0; i < 31; i++) {
				input[i] = Integer.parseInt(words[i+2].split(":")[1]);
			}
			
			int num = getMaxDecrease(input);
			
			map.put(words[1], num);
		}

		bf.close();
		Map<String, Integer> sortedMap = sortByComparator(map);
		int out = printOutput(sortedMap);
		System.out.println(out);
	}
	
	private static int getMaxDecrease(int[] input) {
		List<Integer> temp = new ArrayList<Integer>();
		int counter = 1;
		for (int i = 0; i < 30; i++) {
			if (input[i] > input[i+1]) {
				counter++;
			} else {
				temp.add(counter);
				counter = 1;
			}
		}
		temp.add(counter);
		
		int maxDecrease = 0;
		for (Integer num : temp) {
			if (num > maxDecrease)
				maxDecrease = num;
		}	
		
		return maxDecrease;
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
	 * helper method to print the number of global max of view counts
	 * @param map original map parsed in
	 */
	private static int printOutput(Map<String, Integer> map) {
		List<Integer> out = new ArrayList<Integer>();
		for (Map.Entry<String, Integer> entry : map.entrySet()) {
			out.add(entry.getValue());
		}
		int maxNum = out.get(0);
		int i = 0;
		while (i < out.size()) {
			if (out.get(i) < maxNum) {
				break;
			}
			i++;
		}
		return i;
	}
}
