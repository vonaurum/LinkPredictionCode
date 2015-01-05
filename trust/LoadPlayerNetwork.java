/**
 * 
 */
package linkpred.trust;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import util.ConnectionUtil;

import linkpred.superlearn.bean.BBNNode;
import linkpred.trust.bean.LPNetwork;

/**
 * @author Zoheb H Borbora
 * 
 */
public class LoadPlayerNetwork {

	protected Map<Integer, BBNNode> GLOBAL_PLAYER_MAP = new HashMap<Integer, BBNNode>();
	private int EDGE_COUNT = 0;

	private static String INSERT_WEIGHTED_TRAINING_EDGE = "INSERT INTO lp_training_period_edge"
			+ "(lp_training_period_edge_id, char1_id, char2_id, num_count) VALUES (?, ?, ?, ?)";

	private static String INSERT_WEIGHTED_TEST_EDGE = "INSERT INTO lp_test_period_edge"
			+ "(lp_test_period_edge_id, char1_id, char2_id, num_count) VALUES (?, ?, ?, ?)";

	public void rebuildPlayerNetwork(LPNetwork network, String tableName) {
		System.out.println("*** " + network.getName()
				+ " network: Deleting existing edges from " + tableName);
		deleteExistingPlayerEdges(tableName);
		EDGE_COUNT = 0;
		System.out.println("*** " + network.getName()
				+ " network: Inserting new edges to " + tableName);
		buildPlayerEdges(network, tableName);
		System.out.println(network.getName() + " network: No. of edges = "
				+ EDGE_COUNT);
	}

	private void deleteExistingPlayerEdges(String tableName) {

		Connection conn = getSourceConn();
		try {
			Statement stmt = conn.createStatement();
			String del_stmt = "delete from " + tableName;
			System.out.println("Delete stmt: " + del_stmt);
			stmt.executeUpdate(del_stmt);
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	private void buildPlayerEdges(LPNetwork network, String tableName) {

		Map<String, Integer> weightedEdgeMap = new HashMap<String, Integer>();
		Connection conn = getSourceConn();
		try {
			System.out.println("*** Constructing edge + weights.. ");
			ResultSet rs = getRelevantPairsData(conn, network, tableName);
			while (rs.next()) {
				try {
					long charOneId = rs.getLong(network.getCharOneColumn());
					long charTwoId = rs.getLong(network.getCharTwoColumn());
					String key = getEdgeKey(charOneId, charTwoId);
					int weight = 1;
					if (weightedEdgeMap.containsKey(key)) {
						weight += weightedEdgeMap.get(key);
					}
					weightedEdgeMap.put(key, weight);
				} catch (Exception e) {
					System.out.println("skipping insertion for invalid data");
				}
			}

			PreparedStatement stmt_insert = getInsertStatement(conn, tableName);
			// System.out.println("Edge map size = " + weightedEdgeMap.size());
			for (Map.Entry<String, Integer> mapEntry : weightedEdgeMap
					.entrySet()) {
				String chrs = mapEntry.getKey();
				String[] tokens = chrs.split("_");
				try {
					insertWeightedEdge(stmt_insert, Long.valueOf(tokens[0]),
							Long.valueOf(tokens[1]), mapEntry.getValue());
				} catch (Exception e) {
					System.out.println("Exception while inserting edge: "
							+ chrs + e.getMessage());
				}
			}
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	private ResultSet getRelevantPairsData(Connection conn, LPNetwork network,
			String tableName) throws SQLException {

		String query = "";
		if ("lp_training_period_edge".equals(tableName)) {
			query = "select * from " + network.getTrainingPeriodTable();
		} else if ("lp_test_period_edge".equals(tableName)) {
			query = "select * from " + network.getTestPeriodTable();
		}
		System.out.println("Src query = " + query);
		Statement stmt = conn.createStatement();
		return stmt.executeQuery(query);
	}

	private String getEdgeKey(long charOneId, long charTwoId) {
		long smallerCharId;
		long biggerCharId;
		if (charOneId < charTwoId) {
			smallerCharId = charOneId;
			biggerCharId = charTwoId;
		} else {
			smallerCharId = charTwoId;
			biggerCharId = charOneId;
		}
		return smallerCharId + "_" + biggerCharId;
	}

	private PreparedStatement getInsertStatement(Connection conn,
			String tableName) throws SQLException {

		String insertStmt = "";
		if ("lp_training_period_edge".equals(tableName)) {
			insertStmt = INSERT_WEIGHTED_TRAINING_EDGE;
		} else if ("lp_test_period_edge".equals(tableName)) {
			insertStmt = INSERT_WEIGHTED_TEST_EDGE;
		}
		System.out.println("Insert statement: " + insertStmt);
		return conn.prepareStatement(insertStmt);
	}

	private void insertWeightedEdge(PreparedStatement ps, long charOneId,
			long charTwoId, int weight) throws SQLException {

		ps.setInt(1, ++EDGE_COUNT);
		ps.setLong(2, charOneId);
		ps.setLong(3, charTwoId);
		ps.setInt(4, weight);
		ps.executeUpdate();
		// System.out.println("2 - Inserted " + COUNT_EDGE + " - " + charOneId);
	}

	private Connection getSourceConn() {
		return ConnectionUtil.getGUILETrustConnection(false);
	}
}