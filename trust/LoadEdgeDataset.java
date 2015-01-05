/**
 * 
 */
package linkpred.trust;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import linkpred.ds.Dijkstra;
import linkpred.ds.Node;
import linkpred.trust.bean.LPNetwork;
import linkpred.trust.bean.TrustNode;
import util.ConnectionUtil;
import util.MathUtil;

/**
 * @author zborbor
 * 
 */
public abstract class LoadEdgeDataset {

	protected Map<Long, TrustNode> GLOBAL_PLAYER_MAP = new HashMap<Long, TrustNode>();

	private static String POSITIVE_LABEL = "Y";

	private static String NEGATIVE_LABEL = "N";

	private static int BATCH_SIZE = 500;

	private int EDGE_COUNT = 0;

	private static String INSERT_DATASET_EDGE = "INSERT INTO lp_edge_dataset"
			+ "(lp_edge_dataset_id, player1_char_id, player2_char_id, form_link) VALUES (?, ?, ?, ?)";

	private static String UPDATE_NODE_FEATURES = "UPDATE lp_edge_dataset SET real_gender_indicator=?, "
			+ "game_gender_indicator=?, game_race_indicator=?, real_country_indicator=?, sum_age=?, "
			+ "sum_char_sl_mins=?, diff_age=?, diff_char_sl_mins=?, sum_joining_age=?, diff_joining_age=?, "
			+ "game_class_indicator=?, sum_char_level=?, diff_char_level=?, guild_indicator=?, sum_guild_rank=?, "
			+ "diff_guild_rank=? WHERE lp_edge_dataset_id = ?";

	private static String UPDATE_TOPOLOGICAL_FEATURES = "UPDATE lp_edge_dataset SET diff_degree_cent=?, "
			+ "diff_betweenness_cent=?, diff_closeness_cent=?, diff_eigenvector_cent=?, sum_degree=?, "
			+ "diff_degree=?, shortest_distance=?, sum_clustering_index=?, common_neighbors=?, salton_index=?, "
			+ "jaccard_index=?, sorensen_index=?, aa_index=?, ra_index=? WHERE lp_edge_dataset_id = ?";

	public void deleteAllExistingEdgeSamples() {

		EDGE_COUNT = 0;
		Connection conn = getSourceConn();
		try {
			Statement stmt = conn.createStatement();
			String del_stmt = "delete from lp_edge_dataset";
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

	public int buildPositiveEdgeSamples(int maxSampleSize, String monthRange,
			int serverId) {

		Connection conn_src = getSourceConn();
		Connection conn_dest = getDestConn();
		try {
			/*
			 * fetch edges present in the test period but not in the training
			 * period
			 */
			System.out
					.println("*** Fetching edges present in the test period but not in the training period...");
			String query = "select char1_id,char2_id from lp_test_period_edge "
					+ "EXCEPT select char1_id,char2_id from lp_training_period_edge";
			System.out.println("Src query = " + query);
			Statement stmt = conn_src.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			// get all individual characters present in training period
			System.out
					.println("*** Loading characters present in traning period...");
			Set<Long> trainingCharactersSet = getTrainingPeriodCharacters(
					conn_src, monthRange, serverId);
			PreparedStatement stmt_insert = conn_dest
					.prepareStatement(INSERT_DATASET_EDGE);
			System.out.println("*** Inserting positive samples...");
			while (rs.next()) {
				long charOne = rs.getLong("char1_id");
				long charTwo = rs.getLong("char2_id");
				// make sure each individual character is present during
				// training period
				if (trainingCharactersSet.contains(charOne)
						&& trainingCharactersSet.contains(charTwo)) {
					// insert positive sample!!
					insertEdgeSample(stmt_insert, ++EDGE_COUNT, charOne,
							charTwo, POSITIVE_LABEL);
					conn_dest.commit();
				} else {
					/*
					 * System.out
					 * .println("Test edge not present in training (discarded): "
					 * + charOne + " " + charTwo);
					 */
				}
				if (EDGE_COUNT >= maxSampleSize)
					break;
			}
			conn_src.commit();
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
		System.out.println("*** Completed!!");

		return EDGE_COUNT;
	}

	public int buildNegativeEdgeSamples(int maxTotalSampleSize,
			String monthRange, int serverId) {

		Connection conn_src = getSourceConn();
		Connection conn_dest = getDestConn();
		try {
			int edgeId = 30000;
			Statement stmt = conn_src.createStatement();
			ResultSet rs = stmt
					.executeQuery("SELECT max(lp_edge_dataset_id) FROM lp_edge_dataset");
			if (rs != null && rs.next()) {
				edgeId = rs.getInt(1);
			}
			/*
			 * build an exclusion set of edges from both training and test
			 * period - such edges cannot be a negative sample
			 */
			System.out
					.println("*** Fetching edges present in the both training and test period...");
			String query = "select char1_id,char2_id from lp_test_period_edge "
					+ "UNION select char1_id,char2_id from lp_training_period_edge";
			System.out.println("Src query = " + query);
			ResultSet rsOne = stmt.executeQuery(query);
			Set<String> exclusionSet = new HashSet<String>();
			while (rsOne.next()) {
				exclusionSet.add(rsOne.getInt("char1_id") + "_"
						+ rsOne.getInt("char2_id"));
			}

			// get all individual characters present in training period
			System.out
					.println("*** Loading characters present in traning period...");
			Set<Long> trainingCharactersSet = getTrainingPeriodCharacters(
					conn_src, monthRange, serverId);
			List<Long> trainingCharactersList = new ArrayList<Long>(
					trainingCharactersSet);
			int listSize = trainingCharactersList.size();
			EDGE_COUNT = 0;
			PreparedStatement stmt_insert = conn_dest
					.prepareStatement(INSERT_DATASET_EDGE);
			System.out.println("*** Inserting negative samples...");
			while (edgeId < maxTotalSampleSize) {

				// get account ids of two individual training players at random
				int random = MathUtil.getRandomNumber(0, listSize - 1);
				long t1 = trainingCharactersList.get(random);
				random = MathUtil.getRandomNumber(0, listSize - 1);
				long t2 = trainingCharactersList.get(random);

				// make sure the edge does not ever form a link
				// if (!exclusionSet.contains(charOneId + "_" + charTwoId)) {
				if (!(exclusionSet.contains(t1 + "_" + t2) || exclusionSet
						.contains(t2 + "_" + t1))) {
					// we have a negative sample!!
					insertEdgeSample(stmt_insert, ++edgeId, t1, t2,
							NEGATIVE_LABEL);
					EDGE_COUNT++;
					conn_dest.commit();
					// System.out.println("num = " + COUNT_EDGE_SAMPLE);
				}
			}
			conn_src.commit();
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
		System.out.println("*** Completed!!");

		return EDGE_COUNT;
	}

	public int buildNegativeEdgeSamples1(int maxTotalSampleSize,
			String monthRange, int serverId) {

		return EDGE_COUNT;
	}

	public int buildNegativeEdgeSamples2(int maxTotalSampleSize,
			String monthRange, int serverId) {

		return EDGE_COUNT;
	}

	protected abstract Set<Long> getTrainingPeriodCharacters(Connection conn,
			String monthRange, int serverId) throws SQLException;

	private void insertEdgeSample(PreparedStatement ps, int id, long charOneId,
			long charTwoId, String formLink) throws SQLException {
		ps.setInt(1, id);
		ps.setLong(2, charOneId);
		ps.setLong(3, charTwoId);
		ps.setString(4, formLink);
		ps.executeUpdate();
		// System.out.println("Inserted " + COUNT + " - " + formLink);
	}

	public static void main(String args[]) {

		System.out.println((int) (double) new Double(11.0));
	}

	public void loadFeatures(int featureSet, LPNetwork network,
			String monthRange, int serverId) {

		Connection conn_src = getSourceConn();
		Connection conn_dest = getDestConn();
		try {
			System.out.println("*** Loading players into memory..");
			loadPlayersMap(network, monthRange, serverId);
			System.out.println("*** Loading edges into memory..");
			Statement stmt = conn_src.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT * FROM lp_edge_dataset");

			String updateStmt = "";
			switch (featureSet) {
			case 1:
				System.out.println("*** Loading node features..");
				updateStmt = UPDATE_NODE_FEATURES;
				break;
			case 2:
				System.out.println("*** Loading topological features..");
				// GLOBAL_PLAYER_MAP needs to be loaded before calling
				// this
				loadPlayerGraph();
				// loadJUNGGraph();
				// SHORTEST_PATHS = new DijkstraShortestPath(JUNG_GRAPH);
				updateStmt = UPDATE_TOPOLOGICAL_FEATURES;
				break;
			case 3:
				System.out.println("*** Loading cross-network features..");
				loadPlayerNeighborsMap(monthRange, serverId);
				updateStmt = getXNetworkUpdateStatment();
				break;
			}
			System.out.println("Update statement: " + updateStmt);
			PreparedStatement stmt_update = conn_dest
					.prepareStatement(updateStmt);
			long count = 0;
			while (rs.next()) {
				int edgeId = rs.getInt("lp_edge_dataset_id");
				long playerOneCharId = rs.getLong("player1_char_id");
				long playerTwoCharId = rs.getLong("player2_char_id");
				TrustNode playerOne = GLOBAL_PLAYER_MAP.get(playerOneCharId);
				TrustNode playerTwo = GLOBAL_PLAYER_MAP.get(playerTwoCharId);
				if (playerOne != null && playerTwo != null) {
					switch (featureSet) {
					case 1:
						addEdgeNodeFeaturesToBatch(stmt_update, edgeId,
								playerOne, playerTwo);
						break;
					case 2:
						addEdgeTopologicalFeaturesToBatch(stmt_update, edgeId,
								playerOne, playerTwo);
						break;
					case 3:
						addEdgeCrossNetworkFeaturesToBatch(stmt_update, edgeId,
								playerOneCharId, playerTwoCharId);
						break;
					}
					count++;
					if (count % BATCH_SIZE == 0) {
						System.out.println("FS#" + featureSet
								+ ": Batch committed. count = " + count);
						stmt_update.executeBatch();
						conn_dest.commit();
					}
				} else {
					System.out
							.println("Should not happen - player char not found: "
									+ (playerOne == null ? playerOneCharId : "")
									+ (playerTwo == null ? playerTwoCharId : ""));
				}
			}
			stmt_update.executeBatch();
			conn_dest.commit();
			conn_src.commit();
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
		System.out.println("*** Completed!!");
	}

	protected abstract void loadPlayersMap(LPNetwork network,
			String monthRange, int serverId);

	protected List<Long> getCharacterList(String commaSepCharacters) {

		List<Long> charList = null;
		if (commaSepCharacters != null
				&& commaSepCharacters.trim().length() != 0) {
			String[] accounts = commaSepCharacters.split(",");
			charList = new ArrayList<Long>();
			for (String strAccount : accounts) {
				charList.add(Long.valueOf(strAccount));
			}
		}
		return charList;
	}

	protected abstract String getXNetworkUpdateStatment();

	private void loadPlayerGraph() {

		for (TrustNode player : GLOBAL_PLAYER_MAP.values()) {
			List<Long> neighborCharacters = player.getNeighborCharacters();
			if (neighborCharacters != null) {
				List<Node> neighbors = new ArrayList<Node>();
				for (Long charId : neighborCharacters) {
					TrustNode neighbor = GLOBAL_PLAYER_MAP.get(charId);
					if (neighbor != null && !neighbors.contains(neighbor))
						neighbors.add(neighbor);
				}
				if (neighbors == null) {
					System.out.println("trouble");
				} else
					player.setNeighbors(neighbors);
			}
		}
	}

	protected abstract void loadPlayerNeighborsMap(String monthRange,
			int serverId);

	protected Set<Long> getCharacterSet(String commaSepCharacters) {

		Set<Long> charSet = null;
		if (commaSepCharacters != null
				&& commaSepCharacters.trim().length() != 0) {
			String[] accounts = commaSepCharacters.split(",");
			charSet = new HashSet<Long>();
			for (String strAccount : accounts) {
				charSet.add(Long.valueOf(strAccount));
			}
		}
		return charSet;
	}

	private void addEdgeNodeFeaturesToBatch(PreparedStatement ps, int edgeId,
			TrustNode x, TrustNode y) throws SQLException {

		nullSafeSet(ps, 1, getIndicator(x.getRealGender(), y.getRealGender()),
				Integer.class);
		nullSafeSet(ps, 2, getIndicator(x.getCharGender(), y.getCharGender()),
				Integer.class);
		nullSafeSet(ps, 3, getIndicator(x.getCharRace(), y.getCharRace()),
				Integer.class);
		nullSafeSet(ps, 4, getIndicator(x.getCountry(), y.getCountry()),
				Integer.class);
		nullSafeSet(ps, 5, getSum(x.getAge2011(), y.getAge2011()),
				Integer.class);
		nullSafeSet(
				ps,
				6,
				getSum(x.getTotalSessionLengthMins(),
						y.getTotalSessionLengthMins()), Integer.class);
		nullSafeSet(ps, 7, getAbsDiff(x.getAge2011(), y.getAge2011()),
				Integer.class);
		nullSafeSet(
				ps,
				8,
				getAbsDiff(x.getTotalSessionLengthMins(),
						y.getTotalSessionLengthMins()), Integer.class);
		nullSafeSet(ps, 9, getSum(x.getAgeAtJoining(), y.getAgeAtJoining()),
				Integer.class);
		nullSafeSet(ps, 10,
				getAbsDiff(x.getAgeAtJoining(), y.getAgeAtJoining()),
				Integer.class);
		nullSafeSet(ps, 11,
				getIndicator(x.getCharClassId(), y.getCharClassId()),
				Integer.class);
		// TODO: temp for nagafen dataset
		/*
		 * nullSafeSet(ps, 12, getSum(x.getCsCharLevel(), y.getCsCharLevel()),
		 * Integer.class); nullSafeSet(ps, 13, getAbsDiff(x.getCsCharLevel(),
		 * y.getCsCharLevel()), Integer.class);
		 */

		nullSafeSet(ps, 12, getSum(x.getMaxCharLevel(), y.getMaxCharLevel()),
				Integer.class);
		nullSafeSet(ps, 13,
				getAbsDiff(x.getMaxCharLevel(), y.getMaxCharLevel()),
				Integer.class);
		nullSafeSet(ps, 14, getIndicator(x.getGuildId(), y.getGuildId()),
				Integer.class);
		nullSafeSet(ps, 15, getSum(x.getGuildRank(), y.getGuildRank()),
				Integer.class);
		nullSafeSet(ps, 16, getAbsDiff(x.getGuildRank(), y.getGuildRank()),
				Integer.class);
		ps.setInt(17, edgeId);
		ps.addBatch();
		// ps.executeUpdate();
	}

	private Integer getIndicator(Object x, Object y) {
		if (x == null && y == null) {
			return null;
		} else {
			Integer ret = 1;
			if (x != null && x.equals(y))
				ret = 0;
			return ret;
		}
	}

	private void addEdgeTopologicalFeaturesToBatch(PreparedStatement ps,
			int edgeId, TrustNode x, TrustNode y) throws SQLException {

		nullSafeSet(ps, 1,
				getAbsDiff(x.getDegreeCentrality(), y.getDegreeCentrality()),
				Float.class);
		nullSafeSet(
				ps,
				2,
				getAbsDiff(x.getBetweennessCentrality(),
						y.getBetweennessCentrality()), Float.class);
		nullSafeSet(
				ps,
				3,
				getAbsDiff(x.getClosenessCentrality(),
						y.getClosenessCentrality()), Float.class);
		nullSafeSet(
				ps,
				4,
				getAbsDiff(x.getEigenvectorCentrality(),
						y.getEigenvectorCentrality()), Float.class);
		nullSafeSet(
				ps,
				5,
				getSum(x.getNumNeighborCharacters(),
						y.getNumNeighborCharacters()), Integer.class);
		nullSafeSet(
				ps,
				6,
				getAbsDiff(x.getNumNeighborCharacters(),
						y.getNumNeighborCharacters()), Integer.class);
		nullSafeSet(ps, 7, getShortestDistance(x, y), Integer.class);
		nullSafeSet(ps, 8,
				getSum(x.getClusteringIndex(), y.getClusteringIndex()),
				Float.class);
		nullSafeSet(ps, 9, getCommonNeighbors(x, y), Integer.class);
		nullSafeSet(ps, 10, computeSaltonIndex(x, y), Float.class);
		nullSafeSet(ps, 11, computeJaccardIndex(x, y), Float.class);
		nullSafeSet(ps, 12, computeSorensenIndex(x, y), Float.class);
		nullSafeSet(ps, 13, computeAAIndex(x, y), Float.class);
		nullSafeSet(ps, 14, computeRAIndex(x, y), Float.class);
		ps.setInt(15, edgeId);
		ps.addBatch();
		// ps.executeUpdate();
	}

	private int getShortestDistance(TrustNode x, TrustNode y) {

		Dijkstra dijkstra = new Dijkstra();
		dijkstra.execute(x, y);
		return dijkstra.getShortestPath(x, y);
	}

	private Integer getCommonNeighbors(TrustNode x, TrustNode y) {

		int commonNeigbors = 0;
		Set<Long> zNeighbors = getIntersectNeighbors(x.getNeighborCharacters(),
				y.getNeighborCharacters());
		if (zNeighbors != null) {
			commonNeigbors = zNeighbors.size();
		}
		return commonNeigbors;
	}

	private Float computeAAIndex(TrustNode x, TrustNode y) {

		Float aa_index = null;
		Set<Long> zNeighbors = getIntersectNeighbors(x.getNeighborCharacters(),
				y.getNeighborCharacters());
		if (zNeighbors != null) {
			float retSum = 0;
			for (Long z : zNeighbors) {
				TrustNode playerZ = GLOBAL_PLAYER_MAP.get(z);
				if (playerZ != null) {
					Set<Long> neighbors = new HashSet<Long>(
							playerZ.getNeighborCharacters());
					if (neighbors != null && neighbors.size() > 0) {
						double denom = Math.log10(neighbors.size());
						if (denom != 0) {
							retSum += (1.0 / denom);
						}
					}
				}
			}
			aa_index = new Float(retSum);
		}
		return aa_index;
	}

	private Float computeRAIndex(TrustNode x, TrustNode y) {

		Float ra_index = null;
		Set<Long> zNeighbors = getIntersectNeighbors(x.getNeighborCharacters(),
				y.getNeighborCharacters());
		if (zNeighbors != null) {
			double retSum = 0;
			for (Long z : zNeighbors) {
				TrustNode playerZ = GLOBAL_PLAYER_MAP.get(z);
				if (playerZ != null) {
					Set<Long> neighbors = new HashSet<Long>(
							playerZ.getNeighborCharacters());
					if (neighbors != null && neighbors.size() > 0) {
						retSum += 1.0 / neighbors.size();
					}
				}
			}
			ra_index = new Float(retSum);
		}
		return ra_index;
	}

	private Float computeSaltonIndex(TrustNode x, TrustNode y) {

		Float saltonIndex = null;
		List<Long> xNeighbors = x.getNeighborCharacters();
		List<Long> yNeighbors = y.getNeighborCharacters();
		Set<Long> zNeighbors = getIntersectNeighbors(xNeighbors, yNeighbors);
		if (xNeighbors != null && yNeighbors != null) {
			saltonIndex = 0f;
			if (zNeighbors != null) {
				saltonIndex = new Float(zNeighbors.size()
						/ Math.sqrt(xNeighbors.size() * yNeighbors.size()));
			}
		}
		return saltonIndex;
	}

	private Float computeJaccardIndex(TrustNode x, TrustNode y) {

		Float jaccardIndex = null;
		List<Long> xNeighbors = x.getNeighborCharacters();
		List<Long> yNeighbors = y.getNeighborCharacters();
		Set<Long> intersectNeighbors = getIntersectNeighbors(xNeighbors,
				yNeighbors);
		Set<Long> unionNeighbors = getUnionNeighbors(xNeighbors, yNeighbors);
		if (unionNeighbors != null) {
			jaccardIndex = 0f;
			if (intersectNeighbors != null) {
				jaccardIndex = new Float(intersectNeighbors.size()
						/ unionNeighbors.size());
			}
		}
		return jaccardIndex;
	}

	private Float computeSorensenIndex(TrustNode x, TrustNode y) {

		Float jaccardIndex = null;
		List<Long> xNeighbors = x.getNeighborCharacters();
		List<Long> yNeighbors = y.getNeighborCharacters();
		int numX = xNeighbors != null ? xNeighbors.size() : 0;
		int numY = yNeighbors != null ? yNeighbors.size() : 0;
		int denom = numX + numY;
		if (denom > 0) {
			jaccardIndex = 0f;
			Set<Long> intersectNeighbors = getIntersectNeighbors(xNeighbors,
					yNeighbors);
			if (intersectNeighbors != null) {
				jaccardIndex = new Float((2 * intersectNeighbors.size())
						/ denom);
			}
		}
		return jaccardIndex;
	}

	private Set<Long> getIntersectNeighbors(List<Long> xNeighborsList,
			List<Long> yNeighborsList) {

		Set<Long> xNeighbors = new HashSet<Long>(xNeighborsList);
		Set<Long> yNeighbors = new HashSet<Long>(yNeighborsList);
		Set<Long> retNeighbors = null;
		if (!(xNeighbors == null || xNeighbors.size() == 0
				|| yNeighbors == null || yNeighbors.size() == 0)) {
			retNeighbors = new HashSet<Long>();
			for (Long xNeighbor : xNeighbors) {
				if (yNeighbors.contains(xNeighbor)) {
					retNeighbors.add(xNeighbor);
				}
			}
		}
		return retNeighbors;
	}

	private Set<Long> getUnionNeighbors(List<Long> xNeighborsList,
			List<Long> yNeighborsList) {

		Set<Long> xNeighbors = new HashSet<Long>(xNeighborsList);
		Set<Long> yNeighbors = new HashSet<Long>(yNeighborsList);
		Set<Long> retNeighbors = null;
		if (!(xNeighbors == null || xNeighbors.size() == 0
				|| yNeighbors == null || yNeighbors.size() == 0)) {
			retNeighbors = new HashSet<Long>();
			for (Long xNeighbor : xNeighbors) {
				retNeighbors.add(xNeighbor);
			}
			for (Long yNeighbor : yNeighbors) {
				retNeighbors.add(yNeighbor);
			}
		} else if (xNeighbors != null && xNeighbors.size() > 0) {
			retNeighbors = xNeighbors;
		} else if (yNeighbors != null && yNeighbors.size() > 0) {
			retNeighbors = yNeighbors;
		}
		return retNeighbors;
	}

	protected abstract void addEdgeCrossNetworkFeaturesToBatch(
			PreparedStatement ps, int edgeId, Long char1Id, Long char2Id)
			throws SQLException;

	protected Integer getEdgePresenceIndicator(
			Map<Long, Set<Long>> neighborsMap, Long char1Id, Long char2Id) {
		int ret = 0;
		Set<Long> neighbors = neighborsMap.get(char1Id);
		if (neighbors != null && neighbors.contains(char2Id)) {
			ret = 1;
		}
		return ret;
	}

	private Integer getSum(Integer xVal, Integer yVal) {
		if (xVal == null && yVal == null) {
			return null;
		} else {
			int xn = xVal != null ? xVal : 0;
			int yn = yVal != null ? yVal : 0;
			return xn + yn;
		}
	}

	private Float getSum(Float xVal, Float yVal) {
		if (xVal == null && yVal == null) {
			return null;
		} else {
			float xn = xVal != null ? xVal : 0;
			float yn = yVal != null ? yVal : 0;
			return xn + yn;
		}
	}

	private Integer getAbsDiff(Integer xVal, Integer yVal) {
		if (xVal == null && yVal == null) {
			return null;
		} else {
			int xn = xVal != null ? xVal : 0;
			int yn = yVal != null ? yVal : 0;
			return Math.abs(xn - yn);
		}
	}

	private Float getAbsDiff(Float xVal, Float yVal) {
		if (xVal == null && yVal == null) {
			return null;
		} else {
			float xn = xVal != null ? xVal : 0;
			float yn = yVal != null ? yVal : 0;
			return Math.abs(xn - yn);
		}
	}

	protected void nullSafeSet(PreparedStatement statement, int index,
			Object paramValue, Class clazz) throws SQLException {

		if (paramValue == null) {
			if (Integer.class.equals(clazz) || Long.class.equals(clazz)
					|| Float.class.equals(clazz)) {
				statement.setNull(index, Types.NUMERIC);
			} else if (String.class.equals(clazz)) {
				statement.setNull(index, Types.VARCHAR);
			} else if (Date.class.equals(clazz)) {
				statement.setNull(index, Types.TIMESTAMP);
			}
		} else {

			if (Integer.class.equals(clazz)) {
				statement.setInt(index, (Integer) paramValue);
			} else if (Long.class.equals(clazz)) {
				statement.setLong(index, (Long) paramValue);
			} else if (Float.class.equals(clazz)) {
				statement.setFloat(index, (Float) paramValue);
			} else if (String.class.equals(clazz)) {
				statement.setString(index, (String) paramValue);
			} else if (Date.class.equals(clazz)) {
				statement.setTimestamp(index,
						new Timestamp(((Date) paramValue).getTime()));
			}
		}
	}

	protected Connection getSourceConn() {
		return ConnectionUtil.getGUILETrustConnection(false);
	}

	protected Connection getDestConn() {
		return ConnectionUtil.getGUILETrustConnection(false);
	}
}
