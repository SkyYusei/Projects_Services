package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;
import org.json.JSONArray;

public class ProfileServlet extends HttpServlet {
	private static Connection conn;
	private static Statement stmt;
	
	public ProfileServlet() {
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
    	JSONObject result = new JSONObject();

    	String id = request.getParameter("id");
    	String pwd = request.getParameter("pwd");

    	try{
    		stmt = conn.createStatement();
    		String sql1 = "SELECT password FROM login_info WHERE userid=" + id;
    		ResultSet rs1 = stmt.executeQuery(sql1);
    		String password = null;
    		while(rs1.next()){
    			password  = rs1.getString("password");
    		}
            
    		if (password.equals(pwd)) {
    			String sql2 = "SELECT name, url FROM user_profile WHERE userid=" + id;
    			ResultSet rs2 = stmt.executeQuery(sql2);
    			String name = null, profile = null;
    			while(rs2.next()){
        			name  = rs2.getString("name");
        			profile  = rs2.getString("url");
        		}
    			result.put("name", name);
                result.put("profile", profile);
    		} else {
    			String name = "Unauthorized";
    	        String profile = "#";
    	        result.put("name", name);
                result.put("profile", profile);
    		}
    		
            PrintWriter writer = response.getWriter();
            writer.write(String.format("returnRes(%s)", result.toString()));
            writer.close();
    	}catch(SQLException se){
    		se.printStackTrace();
    	}catch(Exception e){
    		e.printStackTrace();
    	}
    }

    @Override
    protected void doPost(final HttpServletRequest request, final HttpServletResponse response) 
            throws ServletException, IOException {
        doGet(request, response);
    }
}
