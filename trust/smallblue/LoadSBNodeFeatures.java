package linkpred.trust.smallblue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import linkpred.trust.LoadNodeFeatures;
import linkpred.trust.bean.TrustNode;
import etl.bean.Month;

public class LoadSBNodeFeatures extends LoadNodeFeatures {

	private static int BATCH_SIZE = 500;

	private static final String MAX_ID_QUERY = "select MAX(lp_ibm_tp_features_id) AS max_num from lp_ibm_tp_features";

	private static int MAX_ID = 0;

	private static final String INSERT_NEW_IBM_TP_FEATURE = "INSERT INTO lp_ibm_tp_features (lp_ibm_tp_features_id, "
			+ "hashed_id, month_range) VALUES (?, ?, ?)";

	public void loadNodes(Month startMonth, Month endMonth) {

		Connection conn_src = null;
		Connection conn_dest = null;
		try {

			conn_src = getSourceConn();
			conn_dest = getDestConn();
			PreparedStatement stmt_dest = conn_dest
					.prepareStatement(INSERT_NEW_IBM_TP_FEATURE);

			MAX_ID = (int) getMaxId(conn_dest);
			System.out.println("MAX_ID = " + MAX_ID);

			Statement stmt = conn_src.createStatement();
			System.out.println("*** Start: fetch data");
			String SQL_FETCH_ALL_NODES = "select sender_id AS hashed_id from ibm_hashed_timeline_sentiment "
					+ "where month(time_entry) BETWEEN "
					+ Integer.valueOf(startMonth.getNumber())
					+ " and "
					+ Integer.valueOf(endMonth.getNumber())
					+ " UNION select receiver_id AS hashed_id from ibm_hashed_timeline_sentiment "
					+ "where month(time_entry) BETWEEN "
					+ Integer.valueOf(startMonth.getNumber())
					+ " and "
					+ Integer.valueOf(endMonth.getNumber());
			System.out.println("Query: " + SQL_FETCH_ALL_NODES);
			ResultSet rs = stmt.executeQuery(SQL_FETCH_ALL_NODES);
			System.out.println("*** End: fetch data");
			if (rs != null) {
				System.out.println("*** Start: insert data");
				System.out.println("Insert stmt: " + INSERT_NEW_IBM_TP_FEATURE);
				long count = 0;
				while (rs.next()) {
					try {
						TrustNode trustNode = new TrustNode();
						trustNode.setCharacterId(rs.getLong("hashed_id"));
						trustNode.setMonthRange(Month.getMonthsRange(
								startMonth, endMonth));

						addNodeToBatch(stmt_dest, trustNode);
						count++;
						if (count % BATCH_SIZE == 0) {
							stmt_dest.executeBatch();
							conn_dest.commit();
						}
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
				stmt_dest.executeBatch();
				conn_dest.commit();
				System.out.println("*** End: insert data");
			}
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

		System.out.println("Loaded basic static data");
	}

	private void addNodeToBatch(PreparedStatement ps, TrustNode trustNode)
			throws SQLException {

		ps.setInt(1, ++MAX_ID);
		ps.setLong(2, trustNode.getCharacterId());
		nullSafeSet(ps, 3, trustNode.getMonthRange(), String.class);
		ps.addBatch();
	}

	private long getMaxId(Connection conn) throws SQLException {

		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(MAX_ID_QUERY);

		return (rs != null && rs.next()) ? rs.getLong("max_num") : 0;
	}
}
