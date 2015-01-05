/**
 * 
 */
package linkpred.superlearn.generic;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import util.ConnectionUtil;

import linkpred.ds.Dijkstra;
import linkpred.ds.Node;
import linkpred.superlearn.bean.Player;

/**
 * @author Zoheb H Borbora
 * 
 */
public class FeatureSetConstructor {

	protected Map<Integer, Player> GLOBAL_PLAYER_MAP = new HashMap<Integer, Player>();
	protected Map<String, Integer> WEIGHTED_EDGE_MAP = new HashMap<String, Integer>();

	protected void loadPlayerMap(Connection conn) throws SQLException {

		GLOBAL_PLAYER_MAP = new HashMap<Integer, Player>();
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt
				.executeQuery("SELECT * FROM training_period_player");

		while (rs.next()) {

			int characterId = rs.getInt("char_id");
			if (!GLOBAL_PLAYER_MAP.containsKey(characterId)) {

				Player player = new Player(characterId);
				player.setAccountId(rs.getInt("account"));
				player.setAccessLevel(rs.getString("access_level"));
				player.setRealGender(rs.getString("real_gender"));
				player.setCountry(rs.getString("country"));
				player.setAge2006(rs.getInt("age_2006"));
				player.setAgeAtJoining(rs.getInt("age_joining"));
				player.setCharClassId(rs.getInt("char_class_id"));
				player.setCharLevel(rs.getInt("char_level"));
				player.setCharGender(rs.getInt("char_gender"));
				player.setCharRace(rs.getInt("char_race"));
				player.setNumItemsMoved(rs.getInt("num_items_moved"));
				player.setNumItemsPickup(rs.getInt("num_items_pickup"));
				player.setNumItemsPlaced(rs.getInt("num_items_placed"));
				player.setClusteringIndex(rs.getFloat("clustering_index"));
				player.setNeighborCharacters(getCharacterList(rs
						.getString("neighbors")));
				GLOBAL_PLAYER_MAP.put(characterId, player);
			}
		}
	}

	private List<Integer> getCharacterList(String commaSepCharacters) {

		List<Integer> accountList = null;

		if (commaSepCharacters != null
				&& commaSepCharacters.trim().length() != 0) {

			String[] accounts = commaSepCharacters.split(",");
			accountList = new ArrayList<Integer>();
			for (String strAccount : accounts) {
				accountList.add(Integer.valueOf(strAccount));
			}
		}

		return accountList;
	}

	public void constructFeatureSet(int featureSet, boolean pi) {

		Connection conn = ConnectionUtil.getGUILEConnection(false);
		try {

			System.out.println("Loading player map");
			if (featureSet == 2) {
				loadPlayerNetworkData(conn);
			} else {
				loadPlayerMap(conn);
			}

			if (featureSet == 5 || featureSet == 6 || featureSet == 7) {
				System.out.println("Loading weighted edges");
				loadWeightedEdgeMap(conn);
			}

			System.out.println("Completed loading player map");

			Statement stmt = conn.createStatement();
			ResultSet rs = stmt
					.executeQuery("SELECT * FROM training_period_pairs");
			// + " WHERE shortest_ditsance is null AND form_link = 'N'");
			while (rs.next()) {

				int pairsId = rs.getInt("player_pairs_id");
				int playerOneCharId = rs.getInt("player1_char_id");
				int playerTwoCharId = rs.getInt("player2_char_id");

				Player playerOne = GLOBAL_PLAYER_MAP.get(playerOneCharId);
				Player playerTwo = GLOBAL_PLAYER_MAP.get(playerTwoCharId);

				switch (featureSet) {
				case 1:
					updatePlayerPairWithFirstFeatureSet(conn, pairsId,
							playerOne, playerTwo);
					break;
				case 11:
					updatePlayerPairWithHousingFeatureSet(conn, pairsId,
							playerOne, playerTwo);
					break;
				case 2:
					updatePlayerPairWithShortestDistance(conn, pairsId,
							playerOne, playerTwo);
					break;
				case 3:
					updatePlayerPairWithClusteringIndex(conn, pairsId,
							playerOne, playerTwo);
					break;
				case 4:
					updatePlayerPairWithUnweightedEdgeMeasures(conn, pairsId,
							playerOne, playerTwo);
					break;
				case 5:
					updatePlayerPairWithWCNIndex(conn, pairsId, playerOne,
							playerTwo);
					break;
				case 6:
					updatePlayerPairWithWAAIndex(conn, pairsId, playerOne,
							playerTwo);
					break;
				case 7:
					updatePlayerPairWithWRAIndex(conn, pairsId, playerOne,
							playerTwo);
					break;
				}

				if (pi) {
					System.out.println("FeatureSet " + featureSet
							+ ": Updated pair_id " + pairsId);
				}
				conn.commit();
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
	}

	private void updatePlayerPairWithFirstFeatureSet(Connection conn,
			int pairsId, Player x, Player y) throws SQLException {

		PreparedStatement ps = conn
				.prepareStatement("UPDATE  training_period_pairs "
						+ " SET real_gender_indicator = ?, real_country_indicator = ?, "
						+ "game_class_indicator = ?, game_gender_indicator = ?, "
						+ "game_race_indicator = ?, sum_age = ?, sum_joining_age = ?, "
						+ "sum_char_level = ?, sum_neighbors = ? WHERE player_pairs_id = ? ");
		ps.setInt(1, getIndicator(x.getRealGender(), y.getRealGender()));
		ps.setInt(2, getIndicator(x.getCountry(), y.getCountry()));
		ps.setInt(3, getIndicator(x.getCharClassId(), y.getCharClassId()));
		ps.setInt(4, getIndicator(x.getCharGender(), y.getCharGender()));
		ps.setInt(5, getIndicator(x.getCharRace(), y.getCharRace()));
		ps.setInt(6, getSum(x.getAge2006(), y.getAge2006()));
		ps.setInt(7, getSum(x.getAgeAtJoining(), y.getAgeAtJoining()));
		ps.setInt(8, getSum(x.getCharLevel(), y.getCharLevel()));
		ps.setInt(9, sumOfNeighbors(x, y));
		ps.setInt(10, pairsId);
		ps.executeUpdate();
	}

	private int getIndicator(Object x, Object y) {

		if (x == null && y == null) {
			return -99;
		} else {

			int ret = 1;
			if (x != null && x.equals(y))
				ret = 0;

			return ret;
		}
	}

	private int getSum(int x, int y) {

		return x + y;
	}

	private int sumOfNeighbors(Player x, Player y) {

		List<Integer> xNeighbors = x.getNeighborCharacters();
		List<Integer> yNeighbors = y.getNeighborCharacters();
		if (xNeighbors == null && xNeighbors == null) {
			return -99;
		} else {

			int xn = xNeighbors != null ? xNeighbors.size() : 0;
			int yn = yNeighbors != null ? yNeighbors.size() : 0;
			return xn + yn;
		}
	}

	private void updatePlayerPairWithHousingFeatureSet(Connection conn,
			int pairsId, Player x, Player y) throws SQLException {

		PreparedStatement ps = conn
				.prepareStatement("UPDATE housing_training_player_pairs "
						+ "SET sum_actions = ? WHERE player_pairs_id = ?");
		ps.setInt(1, sumOfActions(x, y));
		ps.setInt(2, pairsId);
		ps.executeUpdate();
	}

	private int sumOfActions(Player x, Player y) {
		return x.getNumItemsMoved() + y.getNumItemsMoved()
				+ x.getNumItemsPickup() + y.getNumItemsPickup()
				+ x.getNumItemsPlaced() + y.getNumItemsPlaced();
	}

	private void loadPlayerNetworkData(Connection conn) throws SQLException {

		loadPlayerMap(conn);
		populatePlayerGraph();
	}

	private void populatePlayerGraph() {

		for (Player player : GLOBAL_PLAYER_MAP.values()) {
			List<Integer> neighborCharacters = player.getNeighborCharacters();
			if (neighborCharacters != null) {

				List<Node> neighbors = new ArrayList<Node>();
				for (Integer charId : neighborCharacters) {

					Player neighbor = GLOBAL_PLAYER_MAP.get(charId);
					if (!neighbors.contains(neighbor))
						neighbors.add(neighbor);
				}
				if (neighbors == null) {
					System.out.println("trouble");
				} else
					player.setNeighbors(neighbors);
			}
		}
	}

	private void updatePlayerPairWithShortestDistance(Connection conn,
			int pairsId, Player x, Player y) throws SQLException {

		PreparedStatement ps = conn
				.prepareStatement("UPDATE training_period_pairs "
						+ " SET shortest_ditsance = ? WHERE player_pairs_id = ? ");
		ps.setInt(1, getShortestDistance(x, y));
		ps.setInt(2, pairsId);
		ps.executeUpdate();
	}

	private int getShortestDistance(Player x, Player y) {

		Dijkstra dijkstra = new Dijkstra();
		dijkstra.execute(x, y);
		return dijkstra.getShortestPath(x, y);
	}

	public void computePlayerClusteringIndex() {

		Connection conn = ConnectionUtil.getGUILEConnection(false);
		try {

			System.out.println("Loading player map..");
			loadPlayerMap(conn);
			System.out.println("Completed loading player map");

			for (Player player : GLOBAL_PLAYER_MAP.values()) {
				// System.out.println("CI " + player.getCharacterId());
				populateClusteringIndex(conn, player);
				conn.commit();
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
	}

	private void populateClusteringIndex(Connection conn, Player player)
			throws SQLException {

		init();
		int numTriangles = findTriangles(player);
		int numConnectedTriples = findAllTriples(player);

		if (numConnectedTriples != 0) {

			float clusteringIndex = (float) (3 * numTriangles)
					/ numConnectedTriples;

			PreparedStatement ps = conn
					.prepareStatement("UPDATE training_period_player"
							+ " SET clustering_index = ? WHERE char_id = ?;");
			ps.setFloat(1, clusteringIndex);
			ps.setInt(2, player.getCharacterId());
			ps.executeUpdate();
		}
	}

	private static Set<String> TRIANGLE_SET = new HashSet<String>();
	private static Set<String> TRIPLE_SET = new HashSet<String>();

	private void init() {

		TRIANGLE_SET = new HashSet<String>();
		TRIPLE_SET = new HashSet<String>();
	}

	private int findTriangles(Player player) {

		List<Integer> neighborsOne = player.getNeighborCharacters();
		if (neighborsOne != null && neighborsOne.size() > 0) {
			for (int i = 0; i < neighborsOne.size(); i++) {

				Player neighborOne = GLOBAL_PLAYER_MAP.get(neighborsOne.get(i));
				List<Integer> neighborsTwo = neighborOne
						.getNeighborCharacters();
				if (neighborsTwo != null && neighborsTwo.size() > 0) {
					for (int j = 0; j < neighborsTwo.size(); j++) {

						Player neighborTwo = GLOBAL_PLAYER_MAP.get(neighborsTwo
								.get(j));
						if (neighborTwo.getCharacterId() == player
								.getCharacterId())
							continue;
						List<Integer> neighborsThree = neighborTwo
								.getNeighborCharacters();
						if (neighborsThree != null && neighborsThree.size() > 0) {
							for (int k = 0; k < neighborsThree.size(); k++) {

								Player neighborThree = GLOBAL_PLAYER_MAP
										.get(neighborsThree.get(k));
								if (player == neighborThree) {
									addToTriangleSet(player.getCharacterId(),
											neighborOne.getCharacterId(),
											neighborTwo.getCharacterId());
								}
							}
						}
					}
				}
			}
		}

		return TRIANGLE_SET.size();
	}

	private void addToTriangleSet(Integer one, Integer two, Integer three) {

		List<Integer> triangle = new ArrayList<Integer>();
		triangle.add(one);
		triangle.add(two);
		triangle.add(three);
		Collections.sort(triangle);
		String key = triangle.get(0) + "_" + triangle.get(1) + "_"
				+ triangle.get(2);
		if (!TRIANGLE_SET.contains(key)) {
			System.out.println("triangle " + key);
			TRIANGLE_SET.add(key);
		}
	}

	private int findAllTriples(Player player) {

		findOriginTriples(player);
		findCentredTriples(player);

		return TRIPLE_SET.size();
	}

	private void findOriginTriples(Player player) {

		List<Integer> neighborsOne = player.getNeighborCharacters();
		if (neighborsOne != null && neighborsOne.size() > 0) {
			for (int i = 0; i < neighborsOne.size(); i++) {

				Player neighborOne = GLOBAL_PLAYER_MAP.get(neighborsOne.get(i));
				List<Integer> neighborsTwo = neighborOne
						.getNeighborCharacters();
				if (neighborsTwo != null && neighborsTwo.size() > 0) {
					for (int j = 0; j < neighborsTwo.size(); j++) {

						Player neighborTwo = GLOBAL_PLAYER_MAP.get(neighborsTwo
								.get(j));
						if (neighborTwo.getCharacterId() != player
								.getCharacterId()) {
							addToTripleSet(player.getCharacterId(),
									neighborOne.getCharacterId(),
									neighborTwo.getCharacterId(), true);
						}
					}
				}
			}
		}
	}

	private void findCentredTriples(Player player) {

		List<Integer> neighbors = player.getNeighborCharacters();
		// 1 because at least two neighbors are required to form a
		// centred-triple
		if (neighbors != null && neighbors.size() > 1) {
			// for enumeration
			Collections.sort(neighbors);

			for (int i = 0; i < neighbors.size(); i++) {
				Integer neighborOne = neighbors.get(i);

				for (int j = i + 1; j < neighbors.size(); j++) {
					Integer neighborTwo = neighbors.get(j);

					addToTripleSet(neighborOne, player.getCharacterId(),
							neighborTwo, false);
				}
			}
		}

	}

	private void addToTripleSet(Integer one, Integer two, Integer three,
			boolean isOneAtOrigin) {

		List<Integer> triple = new ArrayList<Integer>();
		triple.add(one);
		triple.add(two);
		triple.add(three);
		Collections.sort(triple);
		String triangleKey = triple.get(0) + "_" + triple.get(1) + "_"
				+ triple.get(2);
		if (TRIANGLE_SET.contains(triangleKey)) {
			TRIPLE_SET.add(triangleKey);
		} else {

			String tripleKey = triple.get(0) + "_" + triple.get(1) + "_"
					+ triple.get(2);
			if (isOneAtOrigin) {
				if (!TRIPLE_SET.contains(tripleKey)) {
					TRIPLE_SET.add(tripleKey);
				}
			}
			// one is at center
			else {

				List<Integer> triple1 = new ArrayList<Integer>();
				triple1.add(one);
				triple1.add(two);
				triple1.add(three);
				if (one > three) {
					Collections.reverse(triple1);
				}
				String key = triple1.get(0) + "_" + triple1.get(1) + "_"
						+ triple1.get(2);
				if (!TRIPLE_SET.contains(key)) {
					TRIPLE_SET.add(key);
				}
			}
		}
	}

	private void updatePlayerPairWithClusteringIndex(Connection conn,
			int pairsId, Player x, Player y) throws SQLException {

		PreparedStatement ps = conn
				.prepareStatement("UPDATE training_period_pairs "
						+ " SET sum_clustering_index = ? WHERE player_pairs_id = ? ");
		ps.setFloat(1, sumClusteringIndex(x, y));
		ps.setInt(2, pairsId);
		ps.executeUpdate();
	}

	private Float sumClusteringIndex(Player x, Player y) {

		Float xClusteringIndex = x.getClusteringIndex();
		Float yClusteringIndex = y.getClusteringIndex();
		if (xClusteringIndex == null && yClusteringIndex == null) {
			return null;
		} else {

			float xn = xClusteringIndex != null ? xClusteringIndex.floatValue()
					: 0;
			float yn = yClusteringIndex != null ? yClusteringIndex.floatValue()
					: 0;
			return (float) xn + yn;
		}
	}

	private Set<Integer> getZNeighbors(Set<Integer> xNeighbors,
			Set<Integer> yNeighbors) {

		Set<Integer> zNeighbors = null;

		if (!(xNeighbors == null || xNeighbors.size() == 0
				|| yNeighbors == null || yNeighbors.size() == 0)) {

			zNeighbors = new HashSet<Integer>();
			for (Integer xNeighbor : xNeighbors) {
				if (yNeighbors.contains(xNeighbor)) {
					zNeighbors.add(xNeighbor);
				}
			}
		}

		return zNeighbors;
	}

	private void updatePlayerPairWithUnweightedEdgeMeasures(Connection conn,
			int pairsId, Player x, Player y) throws SQLException {

		// System.out.println(pairsId + ". CN = " + getCommonNeighbors(x, y));

		PreparedStatement ps = conn
				.prepareStatement("UPDATE training_period_pairs "
						+ " SET common_neighbors = ?, aa_index = ?, ra_index = ? "
						+ "WHERE player_pairs_id = ? ");
		ps.setInt(1, getCommonNeighbors(x, y));
		ps.setFloat(2, computeAAIndex(x, y));
		ps.setFloat(3, computeRAIndex(x, y));
		ps.setInt(4, pairsId);
		ps.executeUpdate();
	}

	private Integer getCommonNeighbors(Player x, Player y) {

		int commonNeigbors = 0;

		Set<Integer> xNeighbors = new HashSet<Integer>(
				x.getNeighborCharacters());
		Set<Integer> yNeighbors = new HashSet<Integer>(
				y.getNeighborCharacters());
		Set<Integer> zNeighbors = getZNeighbors(xNeighbors, yNeighbors);
		if (zNeighbors != null) {
			commonNeigbors = zNeighbors.size();
		}

		return commonNeigbors;
	}

	private Float computeAAIndex(Player x, Player y) {

		Float aa_index = null;

		Set<Integer> xNeighbors = new HashSet<Integer>(
				x.getNeighborCharacters());
		Set<Integer> yNeighbors = new HashSet<Integer>(
				y.getNeighborCharacters());
		Set<Integer> zNeighbors = getZNeighbors(xNeighbors, yNeighbors);
		if (zNeighbors != null) {
			float retSum = 0;
			for (Integer z : zNeighbors) {

				Player playerZ = GLOBAL_PLAYER_MAP.get(z);
				Set<Integer> neighbors = new HashSet<Integer>(
						playerZ.getNeighborCharacters());
				if (neighbors != null && neighbors.size() > 0) {
					double denom = Math.log10(neighbors.size());
					if (denom != 0) {
						retSum += (1.0 / denom);
					}
				}
			}

			aa_index = new Float(retSum);
		}

		return aa_index;
	}

	private Float computeRAIndex(Player x, Player y) {

		Float ra_index = null;

		Set<Integer> xNeighbors = new HashSet<Integer>(
				x.getNeighborCharacters());
		Set<Integer> yNeighbors = new HashSet<Integer>(
				y.getNeighborCharacters());
		Set<Integer> zNeighbors = getZNeighbors(xNeighbors, yNeighbors);
		if (zNeighbors != null) {
			double retSum = 0;
			for (Integer z : zNeighbors) {
				Player playerZ = GLOBAL_PLAYER_MAP.get(z);
				Set<Integer> neighbors = new HashSet<Integer>(
						playerZ.getNeighborCharacters());
				if (neighbors != null && neighbors.size() > 0) {
					retSum += 1.0 / neighbors.size();
				}
			}

			ra_index = new Float(retSum);
		}

		return ra_index;
	}

	protected void loadWeightedEdgeMap(Connection conn) throws SQLException {

		WEIGHTED_EDGE_MAP = new HashMap<String, Integer>();
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT * FROM training_period_edge");

		while (rs.next()) {

			int smallerCharId = rs.getInt("char1_id");
			int biggerCharId = rs.getInt("char2_id");
			String key = getWeightedEdgeKey(smallerCharId, biggerCharId);
			WEIGHTED_EDGE_MAP.put(key, rs.getInt("num_count"));
		}
	}

	private String getWeightedEdgeKey(int charOneId, int charTwoId) {

		int smallerCharId;
		int biggerCharId;

		if (charOneId < charTwoId) {
			smallerCharId = charOneId;
			biggerCharId = charTwoId;
		} else {
			smallerCharId = charTwoId;
			biggerCharId = charOneId;
		}

		return smallerCharId + "_" + biggerCharId;
	}

	private void updatePlayerPairWithWCNIndex(Connection conn, int pairsId,
			Player x, Player y) throws SQLException {

		// System.out.println(pairsId + ". CN = " + getCommonNeighbors(x, y));

		PreparedStatement ps = conn
				.prepareStatement("UPDATE training_period_pairs "
						+ " SET wcn_index = ?, wcn_index2 = ?, wcn_index3 = ?, "
						+ "wcn_index4 = ?, wcn_index_1 = ?, wcn_index_2 = ?, "
						+ "wcn_index_3 = ? , wcn_index_4 = ? WHERE player_pairs_id = ? ");
		ps.setFloat(1, computeWCNIndex(x, y, 1));
		ps.setFloat(2, computeWCNIndex(x, y, 2));
		ps.setFloat(3, computeWCNIndex(x, y, 3));
		ps.setFloat(4, computeWCNIndex(x, y, 4));
		ps.setFloat(5, computeWCNIndex(x, y, -1));
		ps.setFloat(6, computeWCNIndex(x, y, -2));
		ps.setFloat(7, computeWCNIndex(x, y, -3));
		ps.setFloat(8, computeWCNIndex(x, y, -4));
		ps.setInt(9, pairsId);
		ps.executeUpdate();
	}

	private Float computeWCNIndex(Player x, Player y, double alpha) {

		Float wcnIndex = null;

		Set<Integer> xNeighbors = new HashSet<Integer>(
				x.getNeighborCharacters());
		Set<Integer> yNeighbors = new HashSet<Integer>(
				y.getNeighborCharacters());
		Set<Integer> zNeighbors = getZNeighbors(xNeighbors, yNeighbors);
		if (zNeighbors != null) {
			double retSum = 0;
			for (Integer z : zNeighbors) {

				String xzEdge = getWeightedEdgeKey(x.getCharacterId(), z);
				String yzEdge = getWeightedEdgeKey(y.getCharacterId(), z);

				retSum += (Math.pow(WEIGHTED_EDGE_MAP.get(xzEdge), alpha) + Math
						.pow(WEIGHTED_EDGE_MAP.get(yzEdge), alpha));
			}

			wcnIndex = new Float(retSum);
		}

		return wcnIndex;
	}

	private void updatePlayerPairWithWAAIndex(Connection conn, int pairsId,
			Player x, Player y) throws SQLException {

		// System.out.println(pairsId + ". CN = " + getCommonNeighbors(x, y));
		PreparedStatement ps = conn
				.prepareStatement("UPDATE training_period_pairs "
						+ " SET waa_index = ?, waa_index2 = ?, waa_index3 = ?, "
						+ "waa_index4 = ?, waa_index_1 = ?, waa_index_2 = ?, "
						+ "waa_index_3 = ? , waa_index_4 = ? WHERE player_pairs_id = ? ");

		ps.setFloat(1, computeWAAIndex(x, y, 1));
		ps.setFloat(2, computeWAAIndex(x, y, 2));
		ps.setFloat(3, computeWAAIndex(x, y, 3));
		ps.setFloat(4, computeWAAIndex(x, y, 4));
		ps.setFloat(5, computeWAAIndex(x, y, -1));
		ps.setFloat(6, computeWAAIndex(x, y, -2));
		ps.setFloat(7, computeWAAIndex(x, y, -3));
		ps.setFloat(8, computeWAAIndex(x, y, -4));
		ps.setInt(9, pairsId);
		ps.executeUpdate();
	}

	private Float computeWAAIndex(Player x, Player y, double alpha) {

		Float waaIndex = null;

		Set<Integer> xNeighbors = new HashSet<Integer>(
				x.getNeighborCharacters());
		Set<Integer> yNeighbors = new HashSet<Integer>(
				y.getNeighborCharacters());
		Set<Integer> zNeighbors = getZNeighbors(xNeighbors, yNeighbors);
		if (zNeighbors != null) {
			double retSum = 0;
			for (Integer z : zNeighbors) {

				String xzEdge = getWeightedEdgeKey(x.getCharacterId(), z);
				String yzEdge = getWeightedEdgeKey(y.getCharacterId(), z);

				double numerator = (Math.pow(WEIGHTED_EDGE_MAP.get(xzEdge),
						alpha) + Math.pow(WEIGHTED_EDGE_MAP.get(yzEdge), alpha));

				double sumZEdgeWeights = 0.0;
				Player playerZ = GLOBAL_PLAYER_MAP.get(z);
				Set<Integer> neighbors = new HashSet<Integer>(
						playerZ.getNeighborCharacters());
				if (neighbors != null && neighbors.size() > 0) {
					for (Integer neighbor : neighbors) {
						String zEdge = getWeightedEdgeKey(z, neighbor);
						sumZEdgeWeights += WEIGHTED_EDGE_MAP.get(zEdge);
					}
				}
				double denominator = Math.log10(1 + sumZEdgeWeights);
				if (denominator != 0) {
					retSum += (numerator / denominator);
				}
			}

			waaIndex = new Float(retSum);
		}

		return waaIndex;
	}

	private void updatePlayerPairWithWRAIndex(Connection conn, int pairsId,
			Player x, Player y) throws SQLException {

		// System.out.println(pairsId + ". CN = " + getCommonNeighbors(x, y));

		PreparedStatement ps = conn
				.prepareStatement("UPDATE training_period_pairs "
						+ " SET wra_index = ?, wra_index2 = ?, wra_index3 = ?, "
						+ "wra_index4 = ?, wra_index_1 = ?, wra_index_2 = ?, "
						+ "wra_index_3 = ? , wra_index_4 = ? WHERE player_pairs_id = ? ");

		ps.setFloat(1, computeWRAIndex(x, y, 1));
		ps.setFloat(2, computeWRAIndex(x, y, 2));
		ps.setFloat(3, computeWRAIndex(x, y, 3));
		ps.setFloat(4, computeWRAIndex(x, y, 4));
		ps.setFloat(5, computeWRAIndex(x, y, -1));
		ps.setFloat(6, computeWRAIndex(x, y, -2));
		ps.setFloat(7, computeWRAIndex(x, y, -3));
		ps.setFloat(8, computeWRAIndex(x, y, -4));
		ps.setInt(9, pairsId);
		ps.executeUpdate();
	}

	private Float computeWRAIndex(Player x, Player y, double alpha) {

		Float wraIndex = null;

		Set<Integer> xNeighbors = new HashSet<Integer>(
				x.getNeighborCharacters());
		Set<Integer> yNeighbors = new HashSet<Integer>(
				y.getNeighborCharacters());
		Set<Integer> zNeighbors = getZNeighbors(xNeighbors, yNeighbors);
		if (zNeighbors != null) {
			double retSum = 0;
			for (Integer z : zNeighbors) {

				String xzEdge = getWeightedEdgeKey(x.getCharacterId(), z);
				String yzEdge = getWeightedEdgeKey(y.getCharacterId(), z);

				double numerator = (Math.pow(WEIGHTED_EDGE_MAP.get(xzEdge),
						alpha) + Math.pow(WEIGHTED_EDGE_MAP.get(yzEdge), alpha));

				double sumZEdgeWeights = 0.0;
				Player playerZ = GLOBAL_PLAYER_MAP.get(z);
				Set<Integer> neighbors = new HashSet<Integer>(
						playerZ.getNeighborCharacters());
				if (neighbors != null && neighbors.size() > 0) {
					for (Integer neighbor : neighbors) {
						String zEdge = getWeightedEdgeKey(z, neighbor);
						sumZEdgeWeights += WEIGHTED_EDGE_MAP.get(zEdge);
					}
				}
				double denominator = sumZEdgeWeights;
				if (denominator != 0) {
					retSum += (numerator / denominator);
				}
			}

			wraIndex = new Float(retSum);
		}

		return wraIndex;
	}
}
