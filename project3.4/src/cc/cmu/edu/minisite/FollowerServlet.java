package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.json.JSONObject;
import org.json.JSONArray;

public class FollowerServlet extends HttpServlet {
	public static Configuration configuration;
	private static HTable htable;
	
	private static Connection conn;
	private static Statement stmt;
	
    public FollowerServlet() {
    	// connect hbase
    	configuration = HBaseConfiguration.create();
    	try {
    		configuration.addResource("hbase-site.xml");
    		configuration.set("hbase.zookeeper.quorum", "ip-172-31-24-16");
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    	try {
			htable = new HTable(configuration, "followinfo");
		} catch (IOException e) {
			e.printStackTrace();
		}
    	
    	// connect mysql
    	try{
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/task1","task1","");			
		}catch(SQLException se){
			se.printStackTrace();
		}catch(Exception e){
			e.printStackTrace();
		}
    }

    @Override
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {
        String id = request.getParameter("id");

        JSONObject result = new JSONObject();
        JSONArray followers = new JSONArray();

        // use hbase
        Get g = new Get(id.getBytes());
		Result rs = htable.get(g);
		String followerString = "";
		for (KeyValue kv : rs.raw()) {
			if (new String(kv.getQualifier()).equals("follower")) {
				followerString = new String(kv.getValue());
			}
		}
		
		// use mysql
		List<Profile> list = new ArrayList<Profile>();
		try {
			String[] allFollower = followerString.split(" ");
			stmt = conn.createStatement();
	        for (String eachFollower : allFollower) {
	        	String sql2 = "SELECT name, url FROM user_profile WHERE userid=" + eachFollower;
				ResultSet rs2 = stmt.executeQuery(sql2);
				String name = null, url = null;
				while(rs2.next()){
	    			name  = rs2.getString("name");
	    			url  = rs2.getString("url");
	    		}
				Profile p = new Profile(name, url);
				list.add(p);
	        }
		}catch(SQLException se){
    		se.printStackTrace();
    	}catch(Exception e){
    		e.printStackTrace();
    	}
        
		Collections.sort(list);
		for (Profile profile : list) {
			String name = profile.getName();
			String url = profile.getUrl();
			JSONObject follower = new JSONObject();
	        follower.put("name", name);
	        follower.put("profile", url);
	        followers.put(follower);
		}        
        result.put("followers", followers);
        // sample code ends

        PrintWriter writer = response.getWriter();
        writer.write(String.format("returnRes(%s)", result.toString()));
        writer.close();
    }

    @Override
    protected void doPost(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {
        doGet(request, response);
    }
    
    class Profile implements Comparable<Profile>{
    	private String name;
    	private String url;
    	
    	public Profile(String name, String url) {
    		this.name = name;
    		this.url = url;
    	}
    	
    	public String getName() {
    		return this.name;
    	}
    	
    	public String getUrl() {
    		return this.url;
    	}

		@Override
		public int compareTo(Profile o) {
			if (this.name.equals(o.name)) {
				return this.url.compareTo(o.url);
			} else {
				return this.name.compareTo(o.name);
			}
		}
    }
}

