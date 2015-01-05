/**
 * 
 */
package linkpred.trust.smallblue;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;

import network.GraphLink;
import util.ConnectionUtil;

/**
 * @author zborbor
 * 
 */
public class ConstructNetworkMem {

	private static int BATCH_SIZE = 250;

	private static final String INSERT_NEW_ENTRY = "INSERT INTO ibm_hashed_timeline_sentiment "
			+ "(time_entry,sender_id,receiver_id,sign ) values(?,?,?,?)";

	private static Set<GraphLink> POSITIVE_EDGES = new HashSet<GraphLink>();

	private static Set<GraphLink> NEGATIVE_EDGES = new HashSet<GraphLink>();

	private static Set<GraphLink> NEUTRAL_EDGES = new HashSet<GraphLink>();

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		loadSentimentEdges();
		loadNetworks();
		// Calendar cal = getTimestamp("20080214", "175812");
		// System.out.println(cal.getTime());
		// constructNetwork1();
	}

	private static void loadSentimentEdges() {

		Connection conn_src = null;
		try {

			conn_src = getSourceConn();

			String SRC_QUERY = "select * from ibm_hashed_sentiment_recoded_2x";
			Statement stmt = conn_src.createStatement();
			System.out.println("********** Start: Fetch data");
			ResultSet rs = stmt.executeQuery(SRC_QUERY);
			System.out.println("********** End: Fetch data");

			if (rs != null) {
				while (rs.next()) {
					String senderId = rs.getString("sender_id");
					String receiverId = rs.getString("receiver_id");
					String sign = rs.getString("sign");

					long edgeKey = GraphLink.constructKey(
							Long.valueOf(senderId), Long.valueOf(receiverId));
					GraphLink graphLink = new GraphLink(edgeKey, 1, 1);
					if ("1".equals(sign)) {
						POSITIVE_EDGES.add(graphLink);
					}
					if ("-1".equals(sign)) {
						NEGATIVE_EDGES.add(graphLink);
					}
					if ("0".equals(sign)) {
						NEUTRAL_EDGES.add(graphLink);
					}
				}
			}
			System.out.println("Sentiment edges loaded!!");
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				conn_src.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

	}

	private static void loadNetworks() {

		Connection conn_src = null;
		Connection conn_dest = null;
		BufferedWriter bufferedWriter = null;
		try {

			bufferedWriter = new BufferedWriter(
					new FileWriter(
							"C:\\Users\\zborbor\\work\\linkpred\\ibm\\Small Blue dataset\\missing_entries.txt"));
			conn_src = getSourceConn();
			conn_dest = getDestConn();
			conn_dest.setAutoCommit(false);
			PreparedStatement stmt_dest = conn_dest
					.prepareStatement(INSERT_NEW_ENTRY);

			String SRC_QUERY = "select * from ibm_hashed_timeline";
			Statement stmt = conn_src.createStatement();
			System.out.println("********** Start: Fetch data");
			ResultSet rs = stmt.executeQuery(SRC_QUERY);
			System.out.println("********** End: Fetch data");

			if (rs != null) {
				long missCount = 0;
				long insertionCount = 0;
				while (rs.next()) {
					String strDay = rs.getString("date");
					String timeOfDay = rs.getString("hr_of_day");
					String senderId = rs.getString("sender_id");
					String receiverId = rs.getString("receiver_id");
					Calendar cal = getTimestamp(strDay, timeOfDay);

					long edgeKey = GraphLink.constructKey(
							Long.valueOf(senderId), Long.valueOf(receiverId));
					GraphLink graphLink = new GraphLink(edgeKey, 1, 1);
					String sign = null;
					if (POSITIVE_EDGES.contains(graphLink)) {
						sign = "1";
						addToBatch(stmt_dest, cal, Long.valueOf(senderId),
								Long.valueOf(receiverId), Integer.valueOf(sign));
					}
					if (NEGATIVE_EDGES.contains(graphLink)) {
						sign = "-1";
						addToBatch(stmt_dest, cal, Long.valueOf(senderId),
								Long.valueOf(receiverId), Integer.valueOf(sign));
					}
					if (NEUTRAL_EDGES.contains(graphLink)) {
						sign = "0";
						addToBatch(stmt_dest, cal, Long.valueOf(senderId),
								Long.valueOf(receiverId), Integer.valueOf(sign));
					}
					if (sign != null) {
						// System.out.println("Inserted " + senderId + ":"
						// + receiverId);
						insertionCount++;
						if (insertionCount % BATCH_SIZE == 0) {
							stmt_dest.executeBatch();
							conn_dest.commit();
							System.out.println("No. of entries inserted = "
									+ insertionCount);
						}
					} else {
						missCount++;
						bufferedWriter.write("\nNot found " + senderId + ":"
								+ receiverId);
						if (missCount % BATCH_SIZE == 0) {
							bufferedWriter.flush();
						}
					}
				}
				stmt_dest.executeBatch();
				conn_dest.commit();
				bufferedWriter.flush();
				bufferedWriter.close();
				System.out.println("Missed entries = " + missCount);
			}
			System.out.println("Network construction completed!!");
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				conn_src.close();
				conn_dest.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	private static Calendar getTimestamp(String strDay, String timeOfDay) {

		Calendar cal = null;
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd HHmmss");
		try {
			cal = Calendar.getInstance();
			cal.setTime(dateFormat.parse(strDay + " " + timeOfDay));
		} catch (ParseException e) {
			System.out.println("Should not happen - " + strDay + " "
					+ timeOfDay);
			e.printStackTrace();
		}

		return cal;
	}

	private static void addToBatch(PreparedStatement stmt, Calendar entryTime,
			long senderId, long receiverId, int sign) {

		try {
			stmt.setTimestamp(1, new Timestamp(entryTime.getTimeInMillis()));
			stmt.setLong(2, senderId);
			stmt.setLong(3, receiverId);
			stmt.setInt(4, sign);
			stmt.addBatch();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private static Connection getSourceConn() {
		return ConnectionUtil.getGUILETrustConnection();
	}

	private static Connection getDestConn() {
		return ConnectionUtil.getGUILETrustConnection();
	}
}
