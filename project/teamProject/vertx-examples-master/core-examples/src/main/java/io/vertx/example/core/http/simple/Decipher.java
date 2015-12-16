import java.text.SimpleDateFormat;
import java.util.Date;

public class Decipher {
	public Decipher() {

	}
	public static String decipherMessage(String xy, String cipherText) {
		  String teamName = "cmucchackers";
		  String teamId = "363781473979";
		  int xyLastTwoDigit = Integer.parseInt(xy.substring(xy.length() - 2, xy.length()));
		  int shift = 0;
		  if (xyLastTwoDigit == 25 || xyLastTwoDigit == 75) {
		    shift = 1;
		  } else if (xyLastTwoDigit == 23 || xyLastTwoDigit == 73) {
		    shift = 2;
		  } else if (xyLastTwoDigit == 21 || xyLastTwoDigit == 71) {
		    shift = 3;
		  } else if (xyLastTwoDigit == 19 || xyLastTwoDigit == 69) {
		    shift = 4;
		  } else if (xyLastTwoDigit == 17 || xyLastTwoDigit == 67) {
		    shift = 5;
		  } else if (xyLastTwoDigit == 15 || xyLastTwoDigit == 65) {
		    shift = 6;
		  } else if (xyLastTwoDigit == 13 || xyLastTwoDigit == 63) {
		    shift = 7;
		  } else if (xyLastTwoDigit == 11 || xyLastTwoDigit == 61) {
		    shift = 8;
		  } else if (xyLastTwoDigit == 9 || xyLastTwoDigit == 59)  {
		    shift = 9;
		  } else if (xyLastTwoDigit == 7 || xyLastTwoDigit == 57) {
		    shift = 10;
		  } else if (xyLastTwoDigit == 5 || xyLastTwoDigit == 55) {
		    shift = 11;
		  } else if (xyLastTwoDigit == 3 || xyLastTwoDigit == 53) {
		    shift = 12;
		  } else if (xyLastTwoDigit == 1 || xyLastTwoDigit == 51) {
		    shift = 13;
		  } else if (xyLastTwoDigit == 49) {
		    shift = 14;
		  } else if (xyLastTwoDigit == 47 || xyLastTwoDigit == 97) {
		    shift = 15;
		  } else if (xyLastTwoDigit == 45 || xyLastTwoDigit == 95) {
		    shift = 16;
		  } else if (xyLastTwoDigit == 43 || xyLastTwoDigit == 93) {
		    shift = 17;
		  } else if (xyLastTwoDigit == 41 || xyLastTwoDigit == 91) {
		    shift = 18;
		  } else if (xyLastTwoDigit == 39 || xyLastTwoDigit == 89) {
		    shift = 19;
		  } else if (xyLastTwoDigit == 37 || xyLastTwoDigit == 87) {
		    shift = 20;
		  } else if (xyLastTwoDigit == 35 || xyLastTwoDigit == 85) {
		    shift = 21;
		  } else if (xyLastTwoDigit == 33 || xyLastTwoDigit == 83) {
		    shift = 22;
		  } else if (xyLastTwoDigit == 31 || xyLastTwoDigit == 81) {
		    shift = 23;
		  } else if (xyLastTwoDigit == 29 || xyLastTwoDigit == 79) {
		    shift = 24;
		  } else if (xyLastTwoDigit == 27 || xyLastTwoDigit == 77) {
		    shift = 25;
		  }

		  int length = cipherText.length();
		  int size = (int) Math.sqrt(length);
		  Date date = new Date();
		  SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		  String result = teamName + ',' + teamId + '\n' + formatter.format(date) + '\n';
		  for (int i = 0; i < 2 * size - 1; i++) {
		    int length1 = i < size ? 0 : i - size + 1;
		    int length2 = i < size ? 0 : i - size + 1;
		    for (int j = i - length2; j >= length1; j--) {
		      char c = (char) (cipherText.charAt(j + (i - j) * size) - shift);
		      if (c < 65) {
		        c += 26;
		      }
		      result += c;
		    }
		  }
		  return result + '\n';
		};

}
