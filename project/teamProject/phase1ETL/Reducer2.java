import java.io.BufferedReader;
import java.io.InputStreamReader;

public class Reducer2 {
	public static void main(String[] args) throws Exception {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String s;
		String key = "";
		String val = "";
		while ((s = br.readLine()) != null) {
			String[] temp = s.split("\t");
			if (key.equals(temp[0])) {
				val = val + "\\n" + temp[1];
			} else {
				if (key.equals("")) {
					key = temp[0];
					val = temp[1];
				} else {
					System.out.println(key + "\t" + val);
					key = temp[0];
					val = temp[1];
				}
			}
		}
		System.out.println(key + "\t" + val);
		br.close();
	}
}
