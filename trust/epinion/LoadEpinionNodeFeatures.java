package linkpred.trust.epinion;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import linkpred.trust.LoadNodeFeatures;
import linkpred.trust.bean.TrustNode;

public class LoadEpinionNodeFeatures extends LoadNodeFeatures {

	private static int BATCH_SIZE = 500;

	private static final String MAX_ID_QUERY = "select MAX(lp_ep_tp_features_id) AS max_num from lp_ep_tp_features";

	private static int MAX_ID = 0;

	private static final String INSERT_NEW_EP_TP_FEATURE = "INSERT INTO lp_ep_tp_features (lp_ep_tp_features_id, "
			+ "user_id, month_range) VALUES (?, ?, ?)";

	public void loadTrainingNodes() {

		Connection conn_src = null;
		Connection conn_dest = null;
		try {

			conn_src = getSourceConn();
			conn_dest = getDestConn();
			PreparedStatement stmt_dest = conn_dest
					.prepareStatement(INSERT_NEW_EP_TP_FEATURE);

			MAX_ID = (int) getMaxId(conn_dest);
			System.out.println("MAX_ID = " + MAX_ID);

			Statement stmt = conn_src.createStatement();
			System.out.println("*** Start: fetch data");
			String SQL_FETCH_ALL_NODES = "select src_user_id AS user_id from epinion_user_rating where creation_date "
					+ "between '2001-01-01' and '2001-12-31' union select dest_user_id AS user_id "
					+ "from epinion_user_rating where creation_date between '2001-01-01' and '2001-12-31'";
			System.out.println("Query: " + SQL_FETCH_ALL_NODES);
			ResultSet rs = stmt.executeQuery(SQL_FETCH_ALL_NODES);
			System.out.println("*** End: fetch data");
			if (rs != null) {
				System.out.println("*** Start: insert data");
				System.out.println("Insert stmt: " + INSERT_NEW_EP_TP_FEATURE);
				long count = 0;
				while (rs.next()) {
					try {
						TrustNode trustNode = new TrustNode();
						trustNode.setCharacterId(rs.getLong("user_id"));
						trustNode.setMonthRange("training");

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
