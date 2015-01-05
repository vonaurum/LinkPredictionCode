/**
 * 
 */
package linkpred.trust.eq2;

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
public class LoadEQ2EdgeDataset extends LoadEdgeDataset {

	protected Map<Long, Set<Long>> HOUSING_NEIGHBORS_MAP = new HashMap<Long, Set<Long>>();

	protected Map<Long, Set<Long>> MENTORING_NEIGHBORS_MAP = new HashMap<Long, Set<Long>>();

	protected Map<Long, Set<Long>> TRADE_NEIGHBORS_MAP = new HashMap<Long, Set<Long>>();

	protected Map<Long, Set<Long>> GROUP_NEIGHBORS_MAP = new HashMap<Long, Set<Long>>();

	protected Map<Long, Set<Long>> PVP_NEIGHBORS_MAP = new HashMap<Long, Set<Long>>();

	private String UPDATE_EQ2_XNETWORK_FEATURES = "UPDATE lp_edge_dataset SET link_in_housing=?, "
			+ "link_in_mentoring=?, link_in_trade=?, link_in_group=?, link_in_pvp=? WHERE lp_edge_dataset_id=?";

	protected Set<Long> getTrainingPeriodCharacters(Connection conn,
			String monthRange, int serverId) throws SQLException {

		Statement stmt = conn.createStatement();
		// load all distinct individual training players into memory
		String query = "select char_id from lp_player_tp_features f WHERE f.month_range = '"
				+ monthRange
				+ "' AND f.server_id = "
				+ serverId
				+ " INTERSECT ( select char1_id AS char_id from lp_training_period_edge "
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
			String query = "select * from lp_player_tp_features f INNER JOIN "
					+ "(select player1_char_id AS char_id from lp_edge_dataset "
					+ "UNION select player2_char_id AS char_id from lp_edge_dataset) "
					+ "AS t ON f.char_id = t.char_id WHERE f.month_range = '"
					+ monthRange + "' AND f.server_id = " + serverId;
			System.out.println("Query: " + query);
			ResultSet rs = stmt.executeQuery(query);
			System.out.println("** End: fetch data");
			String networkPrefix = network.getFeaturePrefix();
			while (rs.next()) {
				long charId = rs.getLong("char_id");
				if (!GLOBAL_PLAYER_MAP.containsKey(charId)) {
					TrustNode player = new TrustNode(charId);
					player.setServerId(rs.getInt("server_id"));
					player.setAccountId(rs.getLong("account"));
					player.setRealGender(rs.getString("real_gender"));
					player.setCountry(rs.getString("country"));
					player.setAge2006(rs.getInt("age_2006"));
					player.setAgeAtJoining(rs.getInt("age_joining"));
					player.setCharClassId(rs.getInt("char_class_id"));
					player.setCsCharLevel(rs.getInt("cs_char_level"));
					player.setCharGender(rs.getInt("char_gender"));
					player.setCharRace(rs.getInt("char_race"));
					player.setGuildId(rs.getInt("guild_id"));
					player.setGuildRank(rs.getInt("guild_rank"));
					player.setMonthRange(rs.getString("month_range"));
					player.setMaxCharLevel(rs.getInt("max_char_level"));
					player.setTotalSessionLengthMins(rs.getInt("total_sl_mins"));
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

	protected void loadPlayerNeighborsMap(String monthRange, int serverId) {

		Connection conn = getSourceConn();
		try {
			// get computed features of the nodes present in the edge dataset
			Statement stmt = conn.createStatement();
			System.out.println("** Start: fetch data");
			String query = "select * from lp_player_tp_features f INNER JOIN "
					+ "(select player1_char_id AS char_id from lp_edge_dataset "
					+ "UNION select player2_char_id AS char_id from lp_edge_dataset) "
					+ "AS t ON f.char_id = t.char_id WHERE f.month_range = '"
					+ monthRange + "' AND f.server_id = " + serverId;
			System.out.println("Query: " + query);
			ResultSet rs = stmt.executeQuery(query);
			System.out.println("** End: fetch data");
			while (rs.next()) {
				long charId = rs.getLong("char_id");
				HOUSING_NEIGHBORS_MAP.put(charId,
						getCharacterSet(rs.getString("trust_neighbors")));
				MENTORING_NEIGHBORS_MAP.put(charId,
						getCharacterSet(rs.getString("mentoring_neighbors")));
				TRADE_NEIGHBORS_MAP.put(charId,
						getCharacterSet(rs.getString("trade_neighbors")));
				GROUP_NEIGHBORS_MAP.put(charId,
						getCharacterSet(rs.getString("group_neighbors")));
				PVP_NEIGHBORS_MAP.put(charId,
						getCharacterSet(rs.getString("pvp_neighbors")));
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
		return UPDATE_EQ2_XNETWORK_FEATURES;
	}

	protected void addEdgeCrossNetworkFeaturesToBatch(PreparedStatement ps,
			int edgeId, Long char1Id, Long char2Id) throws SQLException {

		nullSafeSet(
				ps,
				1,
				getEdgePresenceIndicator(HOUSING_NEIGHBORS_MAP, char1Id,
						char2Id), Integer.class);
		nullSafeSet(
				ps,
				2,
				getEdgePresenceIndicator(MENTORING_NEIGHBORS_MAP, char1Id,
						char2Id), Integer.class);
		nullSafeSet(
				ps,
				3,
				getEdgePresenceIndicator(TRADE_NEIGHBORS_MAP, char1Id, char2Id),
				Integer.class);
		nullSafeSet(
				ps,
				4,
				getEdgePresenceIndicator(GROUP_NEIGHBORS_MAP, char1Id, char2Id),
				Integer.class);
		nullSafeSet(ps, 5,
				getEdgePresenceIndicator(PVP_NEIGHBORS_MAP, char1Id, char2Id),
				Integer.class);
		ps.setInt(6, edgeId);
		ps.addBatch();
		// ps.executeUpdate();
	}
}
