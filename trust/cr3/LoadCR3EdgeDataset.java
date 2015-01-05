/**
 * 
 */
package linkpred.trust.cr3;

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
public class LoadCR3EdgeDataset extends LoadEdgeDataset {

	protected Map<Long, Set<Long>> FRIEND_NEIGHBORS_MAP = new HashMap<Long, Set<Long>>();

	protected Map<Long, Set<Long>> MENTOR_NEIGHBORS_MAP = new HashMap<Long, Set<Long>>();

	protected Map<Long, Set<Long>> TEAM_NEIGHBORS_MAP = new HashMap<Long, Set<Long>>();

	private String UPDATE_CR3_XNETWORK_FEATURES = "UPDATE lp_edge_dataset SET link_in_friend=?, "
			+ "link_in_mentoring=?, link_in_team=? WHERE lp_edge_dataset_id=?";

	protected Set<Long> getTrainingPeriodCharacters(Connection conn,
			String monthRange, int serverId) throws SQLException {

		Statement stmt = conn.createStatement();
		// load all distinct individual training players into memory
		String query = "select char_id from lp_cr3_tp_features f WHERE f.month_range = '"
				+ monthRange
				+ "' INTERSECT ( select char1_id AS char_id from lp_training_period_edge "
				+ "UNION select char2_id AS char_id from lp_training_period_edge)";
		System.out.println("Query: " + query);
		ResultSet rs = stmt.executeQuery(query);
		Set<Long> trainingCharsSet = new HashSet<Long>();
		while (rs.next()) {
			trainingCharsSet.add(rs.getLong("char_id"));
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
			String query = "select * from lp_cr3_tp_features f INNER JOIN "
					+ "(select player1_char_id AS char_id from lp_edge_dataset "
					+ "UNION select player2_char_id AS char_id from lp_edge_dataset) "
					+ "AS t ON f.char_id = t.char_id WHERE f.month_range = '"
					+ monthRange + "'";
			System.out.println("Query: " + query);
			ResultSet rs = stmt.executeQuery(query);
			System.out.println("** End: fetch data");
			String networkPrefix = network.getFeaturePrefix();
			while (rs.next()) {
				long charId = rs.getLong("char_id");
				if (!GLOBAL_PLAYER_MAP.containsKey(charId)) {

					TrustNode player = new TrustNode(charId);
					player.setAccount(rs.getString("account"));
					player.setRealGender(rs.getString("real_gender"));
					player.setCountry(rs.getString("location"));
					player.setAge2011(nullSafeGet(rs.getObject("age_2011")));
					player.setMonthRange(rs.getString("month_range"));
					player.setMaxCharLevel(nullSafeGet(rs
							.getObject("max_char_level")));
					player.setTotalSessionLengthMins(nullSafeGet(rs
							.getObject("total_sl_mins")));
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
					GLOBAL_PLAYER_MAP.put(charId, player);
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

	private Integer nullSafeGet(Object obj) throws SQLException {

		Integer ret = null;
		if (obj != null && !obj.equals("")) {
			ret = (Integer) obj;
		}

		return ret;
	}

	protected void loadPlayerNeighborsMap(String monthRange, int serverId) {

		Connection conn = getSourceConn();
		try {
			// get computed features of the nodes present in the edge dataset
			Statement stmt = conn.createStatement();
			System.out.println("** Start: fetch data");
			String query = "select * from lp_cr3_tp_features f INNER JOIN "
					+ "(select player1_char_id AS char_id from lp_edge_dataset "
					+ "UNION select player2_char_id AS char_id from lp_edge_dataset) "
					+ "AS t ON f.char_id = t.char_id WHERE f.month_range = '"
					+ monthRange + "'";
			System.out.println("Query: " + query);
			ResultSet rs = stmt.executeQuery(query);
			System.out.println("** End: fetch data");
			while (rs.next()) {
				long charId = rs.getLong("char_id");
				FRIEND_NEIGHBORS_MAP.put(charId,
						getCharacterSet(rs.getString("friend_neighbors")));
				MENTOR_NEIGHBORS_MAP.put(charId,
						getCharacterSet(rs.getString("mentor_neighbors")));
				TEAM_NEIGHBORS_MAP.put(charId,
						getCharacterSet(rs.getString("team_neighbors")));
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
		return UPDATE_CR3_XNETWORK_FEATURES;
	}

	protected void addEdgeCrossNetworkFeaturesToBatch(PreparedStatement ps,
			int edgeId, Long char1Id, Long char2Id) throws SQLException {

		nullSafeSet(
				ps,
				1,
				getEdgePresenceIndicator(FRIEND_NEIGHBORS_MAP, char1Id, char2Id),
				Integer.class);
		nullSafeSet(
				ps,
				2,
				getEdgePresenceIndicator(MENTOR_NEIGHBORS_MAP, char1Id, char2Id),
				Integer.class);
		nullSafeSet(ps, 3,
				getEdgePresenceIndicator(TEAM_NEIGHBORS_MAP, char1Id, char2Id),
				Integer.class);
		ps.setInt(4, edgeId);
		ps.addBatch();
		// ps.executeUpdate();
	}
}
