package cc.cmu.edu.minisite;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONObject;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodb.model.KeySchema;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.datamodeling.*;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;

public class HomepageServlet extends HttpServlet {
	private static AmazonDynamoDBClient client;
	private static DynamoDBMapper mapper;
	
    public HomepageServlet() {
    	client = new AmazonDynamoDBClient(new ProfileCredentialsProvider());
    	mapper = new DynamoDBMapper(client);
    }

    @Override
    protected void doGet(final HttpServletRequest request, 
            final HttpServletResponse response) throws ServletException, IOException {
    	String id = request.getParameter("id");
        JSONObject result = new JSONObject();
        JSONArray array = new JSONArray();
        
        try {            
            Post postKey = new Post();
            postKey.setUserId(Integer.parseInt(id));
            
            DynamoDBQueryExpression<Post> queryExpression = new DynamoDBQueryExpression<Post>().withHashKeyValues(postKey);
                    
            List<Post> posts = mapper.query(Post.class, queryExpression);
            
            for (Post post : posts) {
            	JSONObject postJson = new JSONObject(post.getPost());
            	array.put(postJson);
            }
            
            result.put("posts", array);
            PrintWriter writer = response.getWriter();           
            writer.write(String.format("returnRes(%s)", result.toString()));
            writer.close();
        } catch (Exception e) {
        	e.printStackTrace();
        }
    }

    @Override
    protected void doPost(final HttpServletRequest request, 
            final HttpServletResponse response) throws ServletException, IOException {
        doGet(request, response);
    }
    
    private static BasicAWSCredentials loadProperties() throws IOException {
		// Load the Properties File with AWS Credentials
		Properties properties = new Properties();
		properties.load(HomepageServlet.class
				.getResourceAsStream("/AwsCredentials.properties"));
		BasicAWSCredentials bawsc = new BasicAWSCredentials(
				properties.getProperty("accessKey"),
				properties.getProperty("secretKey"));
		return bawsc;
	}
    
    @DynamoDBTable(tableName="task3")
    public static class Post {
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
    }
}
