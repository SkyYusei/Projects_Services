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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.json.JSONArray;
import org.json.JSONObject;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

import cc.cmu.edu.minisite.FollowerServlet.Profile;
import cc.cmu.edu.minisite.HomepageServlet.Post;

public class TimelineServlet extends HttpServlet {
	public static Configuration configuration;
	private static HTable htable;
	
	private static Connection conn;
	private static Statement stmt;
	
	private static AmazonDynamoDBClient client;
	private static DynamoDBMapper mapper;
	
    public TimelineServlet() throws Exception {
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
    	
    	// dynamoBD
    	client = new AmazonDynamoDBClient(new ProfileCredentialsProvider());
    	mapper = new DynamoDBMapper(client);
    }

    @Override
    protected void doGet(final HttpServletRequest request, 
            final HttpServletResponse response) throws ServletException, IOException {

        JSONObject result = new JSONObject();
        String id = request.getParameter("id");

        // 1
        // use mysql
        try {
        	stmt = conn.createStatement();
        	String sql1 = "SELECT name, url FROM user_profile WHERE userid=" + id;
    		ResultSet rs1 = stmt.executeQuery(sql1);
    		String username = null, userprofile = null;
    		while(rs1.next()){
    			username  = rs1.getString("name");
    			userprofile  = rs1.getString("url");
    		}
    		result.put("name", username);
            result.put("profile", userprofile);
		} catch (Exception e) {
			e.printStackTrace();
		}        
        
        // 2
        // use hbase
        JSONArray followers = new JSONArray();
        Get g = new Get(id.getBytes());
		Result rs = htable.get(g);
		String followerString = "";
		String followeeString = "";
		for (KeyValue kv : rs.raw()) {
			if (new String(kv.getQualifier()).equals("follower")) {
				followerString = new String(kv.getValue());
			}
		}
		for (KeyValue kv : rs.raw()) {
			if (new String(kv.getQualifier()).equals("followee")) {
				followeeString = new String(kv.getValue());
			}
		}
		
		// use mysql
		List<Profile> followerList = new ArrayList<Profile>();
		try {
			String[] allFollower = followerString.split(" ");
	        for (String eachFollower : allFollower) {
	        	String sql2 = "SELECT name, url FROM user_profile WHERE userid=" + eachFollower;
				ResultSet rs2 = stmt.executeQuery(sql2);
				String name = null, url = null;
				while(rs2.next()){
	    			name  = rs2.getString("name");
	    			url  = rs2.getString("url");
	    		}
				Profile p = new Profile(name, url);
				followerList.add(p);
	        }
		}catch(SQLException se){
    		se.printStackTrace();
    	}catch(Exception e){
    		e.printStackTrace();
    	}
        
		Collections.sort(followerList);
		for (Profile profile : followerList) {
			String name = profile.getName();
			String url = profile.getUrl();
			JSONObject follower = new JSONObject();
	        follower.put("name", name);
	        follower.put("profile", url);
	        followers.put(follower);
		}        
        result.put("followers", followers);        
        
        // 3
        JSONArray array = new JSONArray();
        List<Post> followeePostList = new ArrayList<Post>();
        String[] allFollowee = followeeString.split(" ");
        for (String eachFollowee : allFollowee) {
        	try {            
                Post postKey = new Post();
                postKey.setUserId(Integer.parseInt(eachFollowee));
                
                DynamoDBQueryExpression<Post> queryExpression = new DynamoDBQueryExpression<Post>().withHashKeyValues(postKey);
                        
                List<Post> posts = mapper.query(Post.class, queryExpression);
                
                for (Post post : posts) {
                	followeePostList.add(post);
                }                
            } catch (Exception e) {
            	e.printStackTrace();
            }
        }
        
        Collections.sort(followeePostList);
        
        int size = followeePostList.size();
        for (int i = 0; i < 30; i++) {
        	if (i < size) {
        		String post = followeePostList.get(29-i).getPost();
        		JSONObject postJson = new JSONObject(post);
            	array.put(postJson);
        	} else {
        		break;
        	}
        }
        result.put("posts", array);
        
        PrintWriter out = response.getWriter();
        out.print(String.format("returnRes(%s)", result.toString()));
        out.close();
    }

    @Override
    protected void doPost(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
        doGet(req, resp);
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
    
    @DynamoDBTable(tableName="task3")
    public static class Post implements Comparable<Post>{
    	private int userId;
    	private String timestamp;
    	private String post;
    	
    	@DynamoDBHashKey(attributeName="UserID")
		public int getUserId() { return userId; }
		public void setUserId(int userId) { this.userId = userId; }
		
		@DynamoDBRangeKey(attributeName="Timestamp")
		public String getTimestamp() { return timestamp; }
		public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
		
		@DynamoDBAttribute(attributeName="Post")
		public String getPost() { return post; }
		public void setPost(String post) { this.post = post; }
		
		@Override
		public int compareTo(Post o) {
			return this.timestamp.compareTo(o.timestamp)*(-1);
		}
    }
}
