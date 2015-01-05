/**
 * 
 */
package linkpred.superlearn.filmtrust;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
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
import linkpred.superlearn.bean.FTNode;

/**
 * @author Zoheb H Borbora
 * 
 */
public class FeatureSetConstructor {

	private Map<String, FTNode> GLOBAL_NODES_MAP = new HashMap<String, FTNode>();
	private Map<String, Integer> WEIGHTED_EDGE_MAP = new HashMap<String, Integer>();

	private String tableName;

	private String pkColumn;

	public FeatureSetConstructor(String tableName) {
		super();
		this.tableName = tableName;
		this.pkColumn = tableName + "_id";
	}

	protected void loadPlayerMap(Connection conn) throws SQLException {

		GLOBAL_NODES_MAP = new HashMap<String, FTNode>();
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName
				+ " WHERE form_link = 'Y'");

		while (rs.next()) {

			String char1Id = rs.getString("char1_id");
			String char2Id = rs.getString("char2_id");
			FTNode ftNode1 = null;
			if (!GLOBAL_NODES_MAP.containsKey(char1Id)) {
				ftNode1 = new FTNode(char1Id);
				GLOBAL_NODES_MAP.put(char1Id, ftNode1);
			}
			ftNode1 = GLOBAL_NODES_MAP.get(char1Id);
			ftNode1.addToNeighborCharacters(char2Id);

			FTNode ftNode2 = null;
			if (!GLOBAL_NODES_MAP.containsKey(char2Id)) {
				ftNode2 = new FTNode(char2Id);
				GLOBAL_NODES_MAP.put(char2Id, ftNode2);
			}
			ftNode2 = GLOBAL_NODES_MAP.get(char2Id);
			ftNode2.addToNeighborCharacters(char1Id);
		}
	}

	public void constructFeatureSet(int featureSet, boolean pi) {

		Connection conn = ConnectionUtil.getLUIGIConnection(false);
		try {

			System.out.println("Loading player map");
			if (featureSet == 2 || featureSet == 21) {
				loadPlayerNetworkData(conn);
			} else if (featureSet == 3) {
				computePlayerClusteringIndex();
			} else if (featureSet == 5 || featureSet == 6 || featureSet == 7) {
				System.out.println("Loading weighted edges");
				loadWeightedEdgeMap(conn);
			} else {
				loadPlayerMap(conn);
			}

			if (featureSet == 5 || featureSet == 6 || featureSet == 7) {
				System.out.println("Loading weighted edges");
				loadWeightedEdgeMap(conn);
			}

			System.out.println("Completed loading player map");

			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName
					+ " ORDER BY " + pkColumn);
			// + " WHERE shortest_distance is null AND form_link = 'N'");
			while (rs.next()) {

				long pairsId = rs.getLong(pkColumn);
				String playerOneCharId = rs.getString("char1_id");
				String playerTwoCharId = rs.getString("char2_id");

				FTNode playerOne = GLOBAL_NODES_MAP.get(playerOneCharId);
				FTNode playerTwo = GLOBAL_NODES_MAP.get(playerTwoCharId);

				switch (featureSet) {
				case 2:
					updatePlayerPairWithShortestDistance(conn, pairsId,
							playerOne, playerTwo);
					break;
				case 21:
					updatePlayerPairWithShortestDistance1(conn, pairsId,
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

	private int sumOfNeighbors(FTNode x, FTNode y) {

		List<String> xNeighbors = x.getNeighborCharacters();
		List<String> yNeighbors = y.getNeighborCharacters();
		if (xNeighbors == null && xNeighbors == null) {
			return -99;
		} else {

			int xn = xNeighbors != null ? xNeighbors.size() : 0;
			int yn = yNeighbors != null ? yNeighbors.size() : 0;
			return xn + yn;
		}
	}

	private void loadPlayerNetworkData(Connection conn) throws SQLException {

		loadPlayerMap(conn);
		populatePlayerGraph();
	}

	private void populatePlayerGraph() {

		for (FTNode player : GLOBAL_NODES_MAP.values()) {
			List<String> neighborCharacters = player.getNeighborCharacters();
			if (neighborCharacters != null) {

				List<Node> neighbors = new ArrayList<Node>();
				for (String charId : neighborCharacters) {

					FTNode neighbor = GLOBAL_NODES_MAP.get(charId);
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
			long pairsId, FTNode x, FTNode y) throws SQLException {

		PreparedStatement ps = conn.prepareStatement("UPDATE " + tableName
				+ " SET shortest_distance = ? WHERE " + pkColumn + " = ? ");
		ps.setInt(1, getShortestDistance(x, y));
		ps.setLong(2, pairsId);
		ps.executeUpdate();
	}

	private void updatePlayerPairWithShortestDistance1(Connection conn,
			long pairsId, FTNode x, FTNode y) throws SQLException {

		removefromNeighbors(x, y);
		removefromNeighbors(y, x);
		PreparedStatement ps = conn.prepareStatement("UPDATE " + tableName
				+ " SET shortest_distance1 = ? WHERE " + pkColumn + " = ? ");
		ps.setInt(1, getShortestDistance(x, y));
		ps.setLong(2, pairsId);
		ps.executeUpdate();
		addToNeighbors(x, y);
		addToNeighbors(y, x);
	}

	private void removefromNeighbors(FTNode n1, FTNode n2) {

		List<Node> neighbors = n1.getNeighbors();
		if (neighbors.contains(n2)) {
			neighbors.remove(n2);
		}

		List<String> neighborCharacters = n1.getNeighborCharacters();
		if (neighborCharacters.contains(n2.getCharacterId())) {
			neighborCharacters.remove(n2.getCharacterId());
		}
	}

	private void addToNeighbors(FTNode n1, FTNode n2) {

		List<Node> neighbors = n1.getNeighbors();
		if (!neighbors.contains(n2)) {
			neighbors.add(n2);
		}

		List<String> neighborCharacters = n1.getNeighborCharacters();
		if (!neighborCharacters.contains(n2.getCharacterId())) {
			neighborCharacters.add(n2.getCharacterId());
		}
	}

	private int getShortestDistance(FTNode x, FTNode y) {

		Dijkstra dijkstra = new Dijkstra();
		dijkstra.execute(x, y);
		return dijkstra.getShortestPath(x, y);
	}

	public void computePlayerClusteringIndex() {

		Connection conn = ConnectionUtil.getLUIGIConnection(false);
		try {
			System.out.println("Loading player map..");
			loadPlayerMap(conn);
			System.out.println("Completed loading player map");

			for (FTNode ftNode : GLOBAL_NODES_MAP.values()) {
				System.out.println("CI " + ftNode.getCharacterId());
				float ci = getClusteringIndex(ftNode);
				if (ci != -99) {
					ftNode.setClusteringIndex(ci);
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
	}

	private float getClusteringIndex(FTNode ftNode) {

		float clusteringIndex = -99;
		init();
		int numTriangles = findTriangles(ftNode);
		int numConnectedTriples = findAllTriples(ftNode);
		if (numConnectedTriples != 0) {
			clusteringIndex = (float) (3 * numTriangles) / numConnectedTriples;
		}

		return clusteringIndex;
	}

	private static Set<String> TRIANGLE_SET = new HashSet<String>();
	private static Set<String> TRIPLE_SET = new HashSet<String>();

	private void init() {

		TRIANGLE_SET = new HashSet<String>();
		TRIPLE_SET = new HashSet<String>();
	}

	private int findTriangles(FTNode ftNode) {

		List<String> neighborsOne = ftNode.getNeighborCharacters();
		if (neighborsOne != null && neighborsOne.size() > 0) {
			for (int i = 0; i < neighborsOne.size(); i++) {

				FTNode neighborOne = GLOBAL_NODES_MAP.get(neighborsOne.get(i));
				List<String> neighborsTwo = neighborOne.getNeighborCharacters();
				if (neighborsTwo != null && neighborsTwo.size() > 0) {
					for (int j = 0; j < neighborsTwo.size(); j++) {

						FTNode neighborTwo = GLOBAL_NODES_MAP.get(neighborsTwo
								.get(j));
						if (neighborTwo.getCharacterId() == ftNode
								.getCharacterId())
							continue;
						List<String> neighborsThree = neighborTwo
								.getNeighborCharacters();
						if (neighborsThree != null && neighborsThree.size() > 0) {
							for (int k = 0; k < neighborsThree.size(); k++) {

								FTNode neighborThree = GLOBAL_NODES_MAP
										.get(neighborsThree.get(k));
								if (ftNode == neighborThree) {
									addToTriangleSet(ftNode.getCharacterId(),
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

	private void addToTriangleSet(String one, String two, String three) {

		List<String> triangle = new ArrayList<String>();
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

	private int findAllTriples(FTNode player) {

		findOriginTriples(player);
		findCentredTriples(player);

		return TRIPLE_SET.size();
	}

	private void findOriginTriples(FTNode ftNode) {

		List<String> neighborsOne = ftNode.getNeighborCharacters();
		if (neighborsOne != null && neighborsOne.size() > 0) {
			for (int i = 0; i < neighborsOne.size(); i++) {

				FTNode neighborOne = GLOBAL_NODES_MAP.get(neighborsOne.get(i));
				List<String> neighborsTwo = neighborOne.getNeighborCharacters();
				if (neighborsTwo != null && neighborsTwo.size() > 0) {
					for (int j = 0; j < neighborsTwo.size(); j++) {

						FTNode neighborTwo = GLOBAL_NODES_MAP.get(neighborsTwo
								.get(j));
						if (neighborTwo.getCharacterId() != ftNode
								.getCharacterId()) {
							addToTripleSet(ftNode.getCharacterId(), neighborOne
									.getCharacterId(), neighborTwo
									.getCharacterId(), true);
						}
					}
				}
			}
		}
	}

	private void findCentredTriples(FTNode player) {

		List<String> neighbors = player.getNeighborCharacters();
		// 1 because at least two neighbors are required to form a
		// centred-triple
		if (neighbors != null && neighbors.size() > 1) {
			// for enumeration
			Collections.sort(neighbors);
			for (int i = 0; i < neighbors.size(); i++) {
				String neighborOne = neighbors.get(i);
				for (int j = i + 1; j < neighbors.size(); j++) {
					String neighborTwo = neighbors.get(j);
					addToTripleSet(neighborOne, player.getCharacterId(),
							neighborTwo, false);
				}
			}
		}
	}

	private void addToTripleSet(String one, String two, String three,
			boolean isOneAtOrigin) {

		List<String> triple = new ArrayList<String>();
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

				List<String> triple1 = new ArrayList<String>();
				triple1.add(one);
				triple1.add(two);
				triple1.add(three);
				if (one.compareTo(three) > 0) {
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
			long pairsId, FTNode x, FTNode y) throws SQLException {

		PreparedStatement ps = conn.prepareStatement("UPDATE " + tableName
				+ " SET sum_clustering_index = ? WHERE " + pkColumn + " = ? ");
		Float sumCI = sumClusteringIndex(x, y);
		if (sumCI != null) {
			ps.setFloat(1, sumCI);
		} else {
			ps.setNull(1, Types.FLOAT);
		}
		ps.setLong(2, pairsId);
		ps.executeUpdate();
	}

	private Float sumClusteringIndex(FTNode x, FTNode y) {

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

	private Set<String> getZNeighbors(Set<String> xNeighbors,
			Set<String> yNeighbors) {

		Set<String> zNeighbors = null;

		if (!(xNeighbors == null || xNeighbors.size() == 0
				|| yNeighbors == null || yNeighbors.size() == 0)) {

			zNeighbors = new HashSet<String>();
			for (String xNeighbor : xNeighbors) {
				if (yNeighbors.contains(xNeighbor)) {
					zNeighbors.add(xNeighbor);
				}
			}
		}

		return zNeighbors;
	}

	private void updatePlayerPairWithUnweightedEdgeMeasures(Connection conn,
			long pairsId, FTNode x, FTNode y) throws SQLException {

		PreparedStatement ps = conn.prepareStatement("UPDATE " + tableName
				+ " SET common_neighbors = ?, aa_index = ?, ra_index = ? "
				+ "WHERE " + pkColumn + " = ? ");
		ps.setInt(1, getCommonNeighbors(x, y));
		ps.setFloat(2, computeAAIndex(x, y));
		ps.setFloat(3, computeRAIndex(x, y));
		ps.setLong(4, pairsId);
		ps.executeUpdate();
	}

	private Integer getCommonNeighbors(FTNode x, FTNode y) {

		int commonNeigbors = 0;

		Set<String> xNeighbors = new HashSet<String>(x.getNeighborCharacters());
		Set<String> yNeighbors = new HashSet<String>(y.getNeighborCharacters());
		Set<String> zNeighbors = getZNeighbors(xNeighbors, yNeighbors);
		if (zNeighbors != null) {
			commonNeigbors = zNeighbors.size();
		}

		return commonNeigbors;
	}

	private Float computeAAIndex(FTNode x, FTNode y) {

		Float aa_index = null;

		Set<String> xNeighbors = new HashSet<String>(x.getNeighborCharacters());
		Set<String> yNeighbors = new HashSet<String>(y.getNeighborCharacters());
		Set<String> zNeighbors = getZNeighbors(xNeighbors, yNeighbors);
		if (zNeighbors != null) {
			float retSum = 0;
			for (String z : zNeighbors) {

				FTNode playerZ = GLOBAL_NODES_MAP.get(z);
				Set<String> neighbors = new HashSet<String>(playerZ
						.getNeighborCharacters());
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

	private Float computeRAIndex(FTNode x, FTNode y) {

		Float ra_index = null;

		Set<String> xNeighbors = new HashSet<String>(x.getNeighborCharacters());
		Set<String> yNeighbors = new HashSet<String>(y.getNeighborCharacters());
		Set<String> zNeighbors = getZNeighbors(xNeighbors, yNeighbors);
		if (zNeighbors != null) {
			double retSum = 0;
			for (String z : zNeighbors) {
				FTNode playerZ = GLOBAL_NODES_MAP.get(z);
				Set<String> neighbors = new HashSet<String>(playerZ
						.getNeighborCharacters());
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
		ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName);

		while (rs.next()) {

			String smallerCharId = rs.getString("char1_id");
			String biggerCharId = rs.getString("char2_id");
			String key = getWeightedEdgeKey(smallerCharId, biggerCharId);
			WEIGHTED_EDGE_MAP.put(key, rs.getInt("num_count"));
		}
	}

	private String getWeightedEdgeKey(String charOneId, String charTwoId) {

		String smallerCharId;
		String biggerCharId;

		if (charOneId.compareTo(charTwoId) < 0) {
			smallerCharId = charOneId;
			biggerCharId = charTwoId;
		} else {
			smallerCharId = charTwoId;
			biggerCharId = charOneId;
		}

		return smallerCharId + "_" + biggerCharId;
	}

	private void updatePlayerPairWithWCNIndex(Connection conn, long pairsId,
			FTNode x, FTNode y) throws SQLException {

		// System.out.println(pairsId + ". CN = " + getCommonNeighbors(x, y));

		PreparedStatement ps = conn.prepareStatement("UPDATE " + tableName
				+ " SET wcn_index = ?, wcn_index2 = ?, wcn_index3 = ?, "
				+ "wcn_index4 = ?, wcn_index_1 = ?, wcn_index_2 = ?, "
				+ "wcn_index_3 = ? , wcn_index_4 = ? WHERE " + pkColumn
				+ " = ? ");
		ps.setFloat(1, computeWCNIndex(x, y, 1));
		ps.setFloat(2, computeWCNIndex(x, y, 2));
		ps.setFloat(3, computeWCNIndex(x, y, 3));
		ps.setFloat(4, computeWCNIndex(x, y, 4));
		ps.setFloat(5, computeWCNIndex(x, y, -1));
		ps.setFloat(6, computeWCNIndex(x, y, -2));
		ps.setFloat(7, computeWCNIndex(x, y, -3));
		ps.setFloat(8, computeWCNIndex(x, y, -4));
		ps.setLong(9, pairsId);
		ps.executeUpdate();
	}

	private Float computeWCNIndex(FTNode x, FTNode y, double alpha) {

		Float wcnIndex = null;

		Set<String> xNeighbors = new HashSet<String>(x.getNeighborCharacters());
		Set<String> yNeighbors = new HashSet<String>(y.getNeighborCharacters());
		Set<String> zNeighbors = getZNeighbors(xNeighbors, yNeighbors);
		if (zNeighbors != null) {
			double retSum = 0;
			for (String z : zNeighbors) {

				String xzEdge = getWeightedEdgeKey(x.getCharacterId(), z);
				String yzEdge = getWeightedEdgeKey(y.getCharacterId(), z);

				retSum += (Math.pow(WEIGHTED_EDGE_MAP.get(xzEdge), alpha) + Math
						.pow(WEIGHTED_EDGE_MAP.get(yzEdge), alpha));
			}

			wcnIndex = new Float(retSum);
		}

		return wcnIndex;
	}

	private void updatePlayerPairWithWAAIndex(Connection conn, long pairsId,
			FTNode x, FTNode y) throws SQLException {

		// System.out.println(pairsId + ". CN = " + getCommonNeighbors(x, y));
		PreparedStatement ps = conn.prepareStatement("UPDATE " + tableName
				+ " SET waa_index = ?, waa_index2 = ?, waa_index3 = ?, "
				+ "waa_index4 = ?, waa_index_1 = ?, waa_index_2 = ?, "
				+ "waa_index_3 = ? , waa_index_4 = ? WHERE " + pkColumn
				+ " = ? ");

		ps.setFloat(1, computeWAAIndex(x, y, 1));
		ps.setFloat(2, computeWAAIndex(x, y, 2));
		ps.setFloat(3, computeWAAIndex(x, y, 3));
		ps.setFloat(4, computeWAAIndex(x, y, 4));
		ps.setFloat(5, computeWAAIndex(x, y, -1));
		ps.setFloat(6, computeWAAIndex(x, y, -2));
		ps.setFloat(7, computeWAAIndex(x, y, -3));
		ps.setFloat(8, computeWAAIndex(x, y, -4));
		ps.setLong(9, pairsId);
		ps.executeUpdate();
	}

	private Float computeWAAIndex(FTNode x, FTNode y, double alpha) {

		Float waaIndex = null;

		Set<String> xNeighbors = new HashSet<String>(x.getNeighborCharacters());
		Set<String> yNeighbors = new HashSet<String>(y.getNeighborCharacters());
		Set<String> zNeighbors = getZNeighbors(xNeighbors, yNeighbors);
		if (zNeighbors != null) {
			double retSum = 0;
			for (String z : zNeighbors) {

				String xzEdge = getWeightedEdgeKey(x.getCharacterId(), z);
				String yzEdge = getWeightedEdgeKey(y.getCharacterId(), z);

				double numerator = (Math.pow(WEIGHTED_EDGE_MAP.get(xzEdge),
						alpha) + Math.pow(WEIGHTED_EDGE_MAP.get(yzEdge), alpha));

				double sumZEdgeWeights = 0.0;
				FTNode playerZ = GLOBAL_NODES_MAP.get(z);
				Set<String> neighbors = new HashSet<String>(playerZ
						.getNeighborCharacters());
				if (neighbors != null && neighbors.size() > 0) {
					for (String neighbor : neighbors) {
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

	private void updatePlayerPairWithWRAIndex(Connection conn, long pairsId,
			FTNode x, FTNode y) throws SQLException {

		// System.out.println(pairsId + ". CN = " + getCommonNeighbors(x, y));

		PreparedStatement ps = conn.prepareStatement("UPDATE " + tableName
				+ " SET wra_index = ?, wra_index2 = ?, wra_index3 = ?, "
				+ "wra_index4 = ?, wra_index_1 = ?, wra_index_2 = ?, "
				+ "wra_index_3 = ? , wra_index_4 = ? WHERE " + pkColumn
				+ " = ? ");

		ps.setFloat(1, computeWRAIndex(x, y, 1));
		ps.setFloat(2, computeWRAIndex(x, y, 2));
		ps.setFloat(3, computeWRAIndex(x, y, 3));
		ps.setFloat(4, computeWRAIndex(x, y, 4));
		ps.setFloat(5, computeWRAIndex(x, y, -1));
		ps.setFloat(6, computeWRAIndex(x, y, -2));
		ps.setFloat(7, computeWRAIndex(x, y, -3));
		ps.setFloat(8, computeWRAIndex(x, y, -4));
		ps.setLong(9, pairsId);
		ps.executeUpdate();
	}

	private Float computeWRAIndex(FTNode x, FTNode y, double alpha) {

		Float wraIndex = null;

		Set<String> xNeighbors = new HashSet<String>(x.getNeighborCharacters());
		Set<String> yNeighbors = new HashSet<String>(y.getNeighborCharacters());
		Set<String> zNeighbors = getZNeighbors(xNeighbors, yNeighbors);
		if (zNeighbors != null) {
			double retSum = 0;
			for (String z : zNeighbors) {

				String xzEdge = getWeightedEdgeKey(x.getCharacterId(), z);
				String yzEdge = getWeightedEdgeKey(y.getCharacterId(), z);

				double numerator = (Math.pow(WEIGHTED_EDGE_MAP.get(xzEdge),
						alpha) + Math.pow(WEIGHTED_EDGE_MAP.get(yzEdge), alpha));

				double sumZEdgeWeights = 0.0;
				FTNode playerZ = GLOBAL_NODES_MAP.get(z);
				Set<String> neighbors = new HashSet<String>(playerZ
						.getNeighborCharacters());
				if (neighbors != null && neighbors.size() > 0) {
					for (String neighbor : neighbors) {
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
