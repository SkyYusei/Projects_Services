import java.util.List;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.MultiMap;
import org.apache.commons.lang3.StringEscapeUtils;


/*
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class Server extends AbstractVerticle {
	//private Decipher decipher = new Decipher();
	// Convenience method so you can run it in your IDE

	private static EntryDAO entryDAO = new EntryDAO();
		
	public static void main(String[] args) {
		Runner.runExample(Server.class);
	}

	@Override
	public void start() throws Exception {
		vertx.createHttpServer().requestHandler(request -> {
			if (request.method().toString().equals("GET")) {
				if (request.path().equals("") || request.path().equals("/")) {
					request.response().putHeader("content-type", "text/html").end("Welcome to cmucchackers!");
				} else if (request.path().equals("/q2")) {
					MultiMap queries = request.params();
					request.response().putHeader("content-type", "text/html; charset=utf-8").end(query(queries.get("userid"), queries.get("tweet_time")));
				} else {
					request.response().putHeader("content-type", "text/html").end("Wrong query!");
				}
			}
		}).listen(80);
	}

	private static String query(String userId, String tweetTime) {
		String teamName = "cmucchackers";
		String teamId = "363781473979";
		int hashCode = EntryDAO.hashFunc(userId, tweetTime);
		// get the result from the data store with index starting from 0. (same as hashCode)
		String tweets = get(hashCode, userId, tweetTime);
		String result = teamName + ',' + teamId + '\n' + tweets;
		return result;
	}

	private static String get(int hashCode, String userId, String tweetTime) {
		StringBuilder sb = new StringBuilder();
		List<Entry> entries = entryDAO.getEntry(hashCode, userId, tweetTime);
		for (Entry entry : entries) {
			String textDecoded = StringEscapeUtils.unescapeJava(entry.getTweetText());
			entry.setTweetText(textDecoded);
			sb.append(entry.toString());
		}
		return sb.toString();
	}
	
//	private static void connectMySQL() {
//		// We will have many databases
//		for (int i = 0; i < NUM_DATASTORE; i++) {
//			try {
//				Class.forName("com.mysql.jdbc.Driver") ;
//				Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/DBNAME", "usrname", "pswd") ;
//				Statement stmt = conn.createStatement() ;
//				String query = "select columnname from tablename ;" ;
//				ResultSet rs = stmt.executeQuery(query) ;
//				
//			} catch (ClassNotFoundException e) {
//				e.printStackTrace();
//			} catch (SQLException e) {
//				e.printStackTrace();
//			}
//		}
//	}
}
