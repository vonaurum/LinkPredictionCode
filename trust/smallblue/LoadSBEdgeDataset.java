/**
 * 
 */
package linkpred.trust.smallblue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import linkpred.trust.LoadEdgeDataset;
import linkpred.trust.bean.LPNetwork;
import linkpred.trust.bean.TrustNode;

/**
 * @author zborbor
 * 
 */
public class LoadSBEdgeDataset extends LoadEdgeDataset {

	private Map<Long, Set<Long>> POSITIVE_NEIGHBORS_MAP = new HashMap<Long, Set<Long>>();

	private Map<Long, Set<Long>> NEGATIVE_NEIGHBORS_MAP = new HashMap<Long, Set<Long>>();

	private String UPDATE_SB_XNETWORK_FEATURES = "UPDATE lp_edge_dataset SET link_in_pos=?, link_in_neg=? WHERE lp_edge_dataset_id=?";

	protected Set<Long> getTrainingPeriodCharacters(Connection conn,
			String monthRange, int serverId) throws SQLException {

		Statement stmt = conn.createStatement();
		// load all distinct individual training players into memory
		String query = "select hashed_id from lp_ibm_tp_features f WHERE f.month_range = '"
				+ monthRange
				+ "'  INTERSECT ( select char1_id AS hashed_id from lp_training_period_edge "
				+ "UNION select char2_id AS hashed_id from lp_training_period_edge)";
		System.out.println("Query: " + query);
		ResultSet rs = stmt.executeQuery(query);
		Set<Long> trainingCharsSet = new HashSet<Long>();
		while (rs.next()) {
			trainingCharsSet.add(rs.getLong("hashed_id"));
		}

		return trainingCharsSet;
	}

	protected void loadPlayersMap(LPNetwork network, String monthRange,
			int serverId) {

		GLOBAL_PLAYER_MAP = new HashMap<Long, TrustNode>();
		Connection conn = getSourceConn();
		try {

			// get computed features of the nodes present in the edge dataset
			Statement stmt = conn.createStatement();
			System.out.println("** Start: fetch data");
			String query = "select * from lp_ibm_tp_features f INNER JOIN "
					+ "(select player1_char_id AS hashed_id from lp_edge_dataset "
					+ "UNION select player2_char_id AS hashed_id from lp_edge_dataset) AS t "
					+ "ON f.hashed_id = t.hashed_id WHERE f.month_range = '"
					+ monthRange + "'";
			System.out.println("Query: " + query);
			ResultSet rs = stmt.executeQuery(query);
			System.out.println("** End: fetch data");
			String networkPrefix = network.getFeaturePrefix();
			while (rs.next()) {
				long hashedId = rs.getLong("hashed_id");
				if (!GLOBAL_PLAYER_MAP.containsKey(hashedId)) {
					TrustNode player = new TrustNode(hashedId);
					player.setMonthRange(rs.getString("month_range"));
					player.setNeighborCharacters(getCharacterList(rs
							.getString(networkPrefix + "_neighbors")));
					player.setClusteringIndex(rs.getFloat(networkPrefix
							+ "_clustering_index"));
					player.setDegreeCentrality(rs.getFloat(networkPrefix
							+ "_degree_cent"));
					player.setBetweennessCentrality(rs.getFloat(networkPrefix
							+ "_betweenness_cent"));
					player.setClosenessCentrality(rs.getFloat(networkPrefix
							+ "_closeness_cent"));
					player.setEigenvectorCentrality(rs.getFloat(networkPrefix
							+ "_eigenvector_cent"));
					GLOBAL_PLAYER_MAP.put(hashedId, player);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		System.out.println("** No. of players loaded into memory = "
				+ GLOBAL_PLAYER_MAP.size());
	}

	protected void loadPlayerNeighborsMap(String monthRange, int serverId) {

		Connection conn = getSourceConn();
		try {
			// get computed features of the nodes present in the edge dataset
			Statement stmt = conn.createStatement();
			System.out.println("** Start: fetch data");
			String query = "select * from lp_ibm_tp_features f INNER JOIN "
					+ "(select player1_char_id AS hashed_id from lp_edge_dataset "
					+ "UNION select player2_char_id AS hashed_id from lp_edge_dataset) AS t "
					+ "ON f.hashed_id = t.hashed_id WHERE f.month_range = '"
					+ monthRange + "'";
			System.out.println("Query: " + query);
			ResultSet rs = stmt.executeQuery(query);
			System.out.println("** End: fetch data");
			while (rs.next()) {
				long charId = rs.getLong("hashed_id");
				POSITIVE_NEIGHBORS_MAP.put(charId,
						getCharacterSet(rs.getString("pos_neighbors")));
				NEGATIVE_NEIGHBORS_MAP.put(charId,
						getCharacterSet(rs.getString("neg_neighbors")));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		System.out.println("** No. of players loaded into memory = "
				+ GLOBAL_PLAYER_MAP.size());
	}

	@Override
	protected String getXNetworkUpdateStatment() {
		return UPDATE_SB_XNETWORK_FEATURES;
	}

	@Override
	protected void addEdgeCrossNetworkFeaturesToBatch(PreparedStatement ps,
			int edgeId, Long char1Id, Long char2Id) throws SQLException {
		nullSafeSet(
				ps,
				1,
				getEdgePresenceIndicator(POSITIVE_NEIGHBORS_MAP, char1Id,
						char2Id), Integer.class);
		nullSafeSet(
				ps,
				2,
				getEdgePresenceIndicator(NEGATIVE_NEIGHBORS_MAP, char1Id,
						char2Id), Integer.class);
		ps.setInt(3, edgeId);
		ps.addBatch();
	}
}
