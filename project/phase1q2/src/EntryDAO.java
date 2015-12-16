import java.util.List;

import org.hibernate.Criteria;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Restrictions;
import org.hibernate.SessionFactory;

public class EntryDAO {
	private static final int NUM_DATASTORE = 6;
	private static SessionFactory[] factory = new SessionFactory[NUM_DATASTORE];

	public EntryDAO() {
		try {
			// SessionFactories start from 0
			for (int i = 0; i < NUM_DATASTORE; i++) {
				factory[i] = new Configuration()
				.configure("hibernate-mysql" + String.valueOf(i+1) + ".cfg.xml")
				.buildSessionFactory();
				
				// enable emoji
				Session session = factory[i].openSession();
				String sql = "SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci";
				session.createSQLQuery(sql).executeUpdate();
				session.close();
			}
		} catch (Exception e) {
			System.err.println("Failed to create sessionFactory object.");
			return;
		}
	}

	/* Method to CREATE an entry in the database */
	public void addEntry(Integer i, String userId, String tweetTime, String tweetId, String score, String tweetText) {
		// i is from 0
		Session session = factory[i].openSession();
		Transaction tx = null;
		try {
			tx = session.beginTransaction();
			Entry entry = new Entry();
			entry.setUserId(userId);
			entry.setTweetTime(tweetTime);
			entry.setTweetId(tweetId);
			entry.setScore(score);
			entry.setTweetText(tweetText);
			session.save(entry);
			tx.commit();
		} catch (HibernateException e) {
			if (tx != null)
				tx.rollback();
			e.printStackTrace();
		} finally {
			session.close();
		}
	}

	/* Method to UPDATE salary for an employee */
	@SuppressWarnings("unchecked")
	public List<Entry> getEntry(Integer i, String userId, String tweetTime) {
		List<Entry> result = null;	
		Session session = factory[i].openSession();
		Transaction tx = null;
		try {
			tx = session.beginTransaction();			
			
			Criteria cr = session.createCriteria(Entry.class);
			cr.add(Restrictions.eq("userId", userId));
			cr.add(Restrictions.eq("tweetTime", tweetTime));
			cr.addOrder(Order.asc("tweetId"));
			result = (List<Entry>)cr.list();
			
			tx.commit();
		} catch (HibernateException e) {
			if (tx != null)
				tx.rollback();
			e.printStackTrace();
		} finally {
			session.close();
		}
		if (result == null) {
			System.err.println("Failed to get the Entry list with given parameter.");
			return null;
		}
		// Just mark this, and go on.
		if (result.size() == 0) {
			System.out.println("List is empty");
		}
		return result;
	}
	
	public static int hashFunc(String userId, String tweetTime) {
		final int prime = 31;
		long result = 1;
		result = prime * result + ((tweetTime == null) ? 0 : tweetTime.hashCode());
		result = prime * result + ((userId == null) ? 0 : userId.hashCode());
		return (int)(Math.abs(result) % NUM_DATASTORE);
	}
}
