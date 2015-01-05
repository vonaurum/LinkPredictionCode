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

import util.ConnectionUtil;

/**
 * @author zborbor
 * 
 */
public class ConstructNetwork {

	private static int BATCH_SIZE = 250;

	private static final String INSERT_NEW_ENTRY = "INSERT INTO ibm_hashed_timeline_sentiment "
			+ "(time_entry,sender_id,receiver_id,sign ) values(?,?,?,?)";

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		constructNetwork();
		// Calendar cal = getTimestamp("20080214", "175812");
		// System.out.println(cal.getTime());
		// constructNetwork1();
	}

	private static void constructNetwork() {

		Connection conn_src = null;
		Connection conn_dest = null;
		BufferedWriter bufferedWriter = null;
		try {

			bufferedWriter = new BufferedWriter(
					new FileWriter(
							"C:\\zborbor\\work\\linkpred\\input networks\\sb_missing_entries.txt"));
			conn_src = getSourceConn();
			conn_dest = getDestConn();
			PreparedStatement stmt_dest = conn_dest
					.prepareStatement(INSERT_NEW_ENTRY);

			String SRC_QUERY = "select * from ibm_hashed_timeline";
			Statement stmt = conn_src.createStatement();
			System.out.println("********** Start: Fetch data");
			ResultSet rs = stmt.executeQuery(SRC_QUERY);
			System.out.println("********** End: Fetch data");

			if (rs != null) {
				int missCount = 0;
				long insertionCount = 0;
				while (rs.next()) {
					String strDay = rs.getString("date");
					String timeOfDay = rs.getString("hr_of_day");
					String senderId = rs.getString("sender_id");
					String receiverId = rs.getString("receiver_id");
					Calendar cal = getTimestamp(strDay, timeOfDay);
					String query1 = "select * from ibm_hashed_sentiment_recoded_2x where (sender_id = "
							+ senderId
							+ " and receiver_id = "
							+ receiverId
							+ ") or (sender_id = "
							+ senderId
							+ " and receiver_id = " + receiverId + ")";
					Statement stmt1 = conn_src.createStatement();
					ResultSet rs1 = stmt1.executeQuery(query1);
					if (rs1 != null) {

						boolean found = false;
						while (rs1.next()) {
							found = true;
							String sign = rs1.getString("sign");
							addToBatch(stmt_dest, cal, Long.valueOf(senderId),
									Long.valueOf(receiverId),
									Integer.valueOf(sign));
							// System.out.println("Inserted " + senderId + ":"
							// + receiverId);
							insertionCount++;

							if (insertionCount % BATCH_SIZE == 0) {
								stmt_dest.executeBatch();
								conn_dest.commit();
								System.out.println("No. of entries inserted = "
										+ insertionCount);
							}
						}
						if (!found) {
							missCount++;
							bufferedWriter.write("\nNot found " + senderId
									+ ":" + receiverId);
							if (missCount % BATCH_SIZE == 0) {
								bufferedWriter.flush();
							}
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
		return ConnectionUtil.getGUILETrustConnection(false);
	}
}
