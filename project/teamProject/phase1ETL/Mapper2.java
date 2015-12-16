import java.io.BufferedReader;
import java.io.InputStreamReader;

public class Mapper2 {
	public static void main(String[] args) throws Exception {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String s;
		while ((s = br.readLine()) != null) {
			String[] temp = s.split("\t");
			String id_time = temp[0] + temp[1];
			String other = temp[2] + ":" + temp[3] + ":" + temp[4];
			System.out.println(id_time + "\t" + other);
		}
		br.close();
	}

}
