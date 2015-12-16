import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class KeyValueLib {
    public static HashMap<String, Integer> dataCenters = new HashMap();
    public static HashMap<String, Integer> coordinators = new HashMap();
    private static int forwardcount = 0;
    public static final int region = 1;

    private static String URLHandler(String string) throws IOException {
        StringBuilder stringBuilder = new StringBuilder();
        try {
            String string2;
            URL uRL = new URL(string);
            HttpURLConnection httpURLConnection = (HttpURLConnection)uRL.openConnection();
            if (httpURLConnection.getResponseCode() != 200) {
                throw new IOException(httpURLConnection.getResponseMessage());
            }
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(httpURLConnection.getInputStream()));
            while ((string2 = bufferedReader.readLine()) != null) {
                stringBuilder.append(string2);
            }
            bufferedReader.close();
            httpURLConnection.disconnect();
        }
        catch (MalformedURLException var2_3) {
            var2_3.printStackTrace();
        }
        return stringBuilder.toString();
    }

    public static void PUT(String string, String string2, String string3, String string4, String string5) throws IOException {
        Long l = Long.parseLong(string4);
        try {
            switch (dataCenters.get(string)) {
                case 1: {
                    break;
                }
                case 2: {
                    l = l + 200;
                    Thread.sleep(200);
                    break;
                }
                case 3: {
                    l = l + 600;
                    Thread.sleep(600);
                    break;
                }
            }
        }
        catch (InterruptedException var6_6) {
            // empty catch block
        }
        String string6 = String.format("http://%s:8080/put?key=%s&value=%s&timestamp=%s&region=%s&consistency=%s", string, string2, string3, l.toString(), Integer.toString(1), string5);
        String string7 = KeyValueLib.URLHandler(string6);
        if (!string7.equals("stored")) {
            System.out.println("An error occured with the Datacenters.");
        }
    }

    public static void FORWARD(String string, String string2, String string3, String string4) throws IOException {
        Long l = Long.parseLong(string4);
        ++forwardcount;
        try {
            switch (coordinators.get(string)) {
                case 1: {
                    break;
                }
                case 2: {
                    l = l + 200;
                    Thread.sleep(200);
                    break;
                }
                case 3: {
                    l = l + 600;
                    Thread.sleep(600);
                    break;
                }
            }
        }
        catch (InterruptedException var5_5) {
            // empty catch block
        }
        String string5 = String.format("http://%s:8080/put?key=%s&value=%s&timestamp=%s&region=%s&forward=%s", string, string2, string3, l.toString(), Integer.toString(1), "true");
        String string6 = KeyValueLib.URLHandler(string5);
    }

    public static String GET(String string, String string2, String string3, String string4) throws IOException {
        String string5 = String.format("http://%s:8080/get?key=%s&timestamp=%s&consistency=%s", string, string2, string3, string4);
        String string6 = KeyValueLib.URLHandler(string5);
        return string6;
    }

    private static String getDCByRegion(int n) {
        for (Map.Entry<String, Integer> entry : dataCenters.entrySet()) {
            if (!entry.getValue().equals(n)) continue;
            return entry.getKey();
        }
        System.out.println("No DC Found!");
        return "null";
    }

    public static void AHEAD(String string, String string2) throws IOException {
        String string3 = String.format("http://%s:8080/ahead?key=%s&timestamp=%s", KeyValueLib.getDCByRegion(1), string, string2);
        String string4 = KeyValueLib.URLHandler(string3);
        string3 = String.format("http://%s:8080/ahead?key=%s&timestamp=%s", KeyValueLib.getDCByRegion(2), string, string2);
        string4 = KeyValueLib.URLHandler(string3);
        string3 = String.format("http://%s:8080/ahead?key=%s&timestamp=%s", KeyValueLib.getDCByRegion(3), string, string2);
        string4 = KeyValueLib.URLHandler(string3);
    }

    public static void COMPLETE(String string, String string2) throws IOException {
        String string3 = String.format("http://%s:8080/complete?key=%s&timestamp=%s", KeyValueLib.getDCByRegion(1), string, string2);
        String string4 = KeyValueLib.URLHandler(string3);
        string3 = String.format("http://%s:8080/complete?key=%s&timestamp=%s", KeyValueLib.getDCByRegion(2), string, string2);
        string4 = KeyValueLib.URLHandler(string3);
        string3 = String.format("http://%s:8080/complete?key=%s&timestamp=%s", KeyValueLib.getDCByRegion(3), string, string2);
        string4 = KeyValueLib.URLHandler(string3);
    }

    public static String COUNT() {
        return Integer.toString(forwardcount);
    }

    public static void RESET() {
        forwardcount = 0;
    }
}