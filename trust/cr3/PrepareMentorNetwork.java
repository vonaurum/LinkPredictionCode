/*
 * linkpred.trust.cr3.PrepareTeamNetwork.java
 *
 * Created on Feb 26, 2012
 */
package linkpred.trust.cr3;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import util.ConnectionUtil;

/**
 * @author zborbor
 */
public class PrepareMentorNetwork {

	private static int BATCH_SIZE = 1000;

	private static final String TRAINING_PERIOD_NETWORK = "select * from cr3_actions_mentor where"
			+ " actiontype = 49 and month(actiontime) IN (5,6,7)";

	private static final String TEST_PERIOD_NETWORK = "select * from cr3_actions_mentor where"
			+ " actiontype = 49 and month(actiontime) IN (8,9)";

	private static String INSERT_MENTOR_TRAINING_EDGE = "INSERT INTO cr3_mentor_may_jul (src_char_id, dest_char_id) VALUES(?,?)";

	private static String INSERT_MENTOR_TEST_EDGE = "INSERT INTO cr3_mentor_aug_sep (src_char_id, dest_char_id) VALUES(?,?)";

	private static String SRC_QUERY = TRAINING_PERIOD_NETWORK;

	private static String INSERT_ENTRY = INSERT_MENTOR_TRAINING_EDGE;

	private static Set<String> EDGE_SET = new HashSet<String>();

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new PrepareMentorNetwork().run();
	}

	private void run() {
		System.out.println("********* Loading edges..");
		loadNetwork();
		System.out.println("********* Finished!!");
	}

	private void loadNetwork() {

		Connection conn_src = null;
		Connection conn_dest = null;
		try {

			conn_src = getSourceConn();
			conn_dest = getDestConn();
			conn_dest.setAutoCommit(false);
			PreparedStatement stmt_dest = conn_dest
					.prepareStatement(INSERT_ENTRY);
			Statement stmt = conn_src.createStatement();

			System.out.println("***** Loading edge map..");

			System.out.println("***** Start: Fetch data");
			System.out.println(SRC_QUERY);
			ResultSet rs = stmt.executeQuery(SRC_QUERY);
			System.out.println("***** End: Fetch data");
			long prevTimeCount = System.currentTimeMillis() / 1000;
			long currTimeCount = System.currentTimeMillis() / 1000;

			if (rs != null) {
				long count = 0;
				while (rs.next()) {
					try {
						Long apprenticeId = rs.getLong("ROLEINDEXID");
						Long mentorId = Long.valueOf(rs
								.getString("ACTIONTARGET"));
						EDGE_SET.add(mentorId + "_" + apprenticeId);
						if (++count % BATCH_SIZE == 0) {
							// System.out.println("** loadEdge: " + count);
						}
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			}

			System.out.println("***** Inserting edges..");

			long count = 0;
			Iterator<String> edgeItr = EDGE_SET.iterator();
			while (edgeItr.hasNext()) {
				String edge = edgeItr.next();
				String[] s = edge.split("_");
				addToBatch(stmt_dest, Long.valueOf(s[0]), Long.valueOf(s[1]));
				if (++count % BATCH_SIZE == 0) {
					stmt_dest.executeBatch();
					conn_dest.commit();
					currTimeCount = System.currentTimeMillis() / 1000;
					System.out.println("No. of edges written = " + count
							+ " in time(secs) "
							+ (currTimeCount - prevTimeCount));
					prevTimeCount = currTimeCount;
				}
			}
			stmt_dest.executeBatch();
			conn_dest.commit();

		} catch (SQLException e) {
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

	private void addToBatch(PreparedStatement stmt, Long srcCharId,
			Long destCharId) throws SQLException {

		stmt.setLong(1, srcCharId);
		stmt.setLong(2, destCharId);
		stmt.addBatch();
	}

	private static Connection getSourceConn() {
		return ConnectionUtil.getHedgeHogConnection();
	}

	private static Connection getDestConn() {
		return ConnectionUtil.getGUILETrustConnection();
	}

}
