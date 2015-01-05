package linkpred.trust.eq2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import network.GraphLink;
import edu.uci.ics.jung.algorithms.scoring.BetweennessCentrality;
import edu.uci.ics.jung.algorithms.scoring.ClosenessCentrality;
import edu.uci.ics.jung.algorithms.scoring.EigenvectorCentrality;
import edu.uci.ics.jung.graph.UndirectedGraph;
import edu.uci.ics.jung.graph.UndirectedSparseGraph;

import linkpred.trust.bean.LPNetwork;
import linkpred.trust.bean.TrustNode;
import util.ConnectionUtil;

public class LoadPlayerNetworkFeatures {

	private Map<Long, TrustNode> GLOBAL_PLAYER_MAP = new HashMap<Long, TrustNode>();

	private UndirectedGraph<Long, GraphLink> JUNG_GRAPH = new UndirectedSparseGraph<Long, GraphLink>();

	public void loadPlayerNeighbors(LPNetwork network, String monthRange,
			int serverId) {

		System.out.println("*** Loading raw player network into memory...");
		loadPlayersMap();
		System.out.println("*** Saving player neighbors...");
		Connection conn = getDestConn();
		try {
			PreparedStatement stmt_update = getUpdateStatement(conn, network,
					"neighbors");
			for (TrustNode player : GLOBAL_PLAYER_MAP.values()) {
				updatePlayerNeighbors(stmt_update,
						getCommaSepCharacters(player.getNeighborCharacters()),
						serverId, monthRange, player.getCharacterId());
			}
			conn.commit();
		} catch (SQLException e) {
			System.out.println();
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		System.out.println("*** No. of players in network = "
				+ GLOBAL_PLAYER_MAP.size());
	}

	private void updatePlayerNeighbors(PreparedStatement ps,
			String commaSepNeighbors, int serverId, String monthRange,
			long charId) throws SQLException {
		nullSafeSet(ps, 1, commaSepNeighbors, String.class);
		ps.setInt(2, serverId);
		ps.setString(3, monthRange);
		ps.setLong(4, charId);
		ps.executeUpdate();
	}

	public void loadPlayerClusteringIndex(LPNetwork network, String monthRange,
			int serverId) {

		System.out.println("*** Loading raw player network into memory...");
		loadPlayersMap();
		System.out
				.println("*** Computing clustering index for all player nodes...");
		Connection conn = getDestConn();
		try {
			PreparedStatement stmt_update = getUpdateStatement(conn, network,
					"clustering_index");
			for (TrustNode player : GLOBAL_PLAYER_MAP.values()) {
				// System.out.println("CI " + player.getCharacterId());
				updatePlayerClusteringIndex(stmt_update, player, serverId,
						monthRange);
				conn.commit();
			}
		} catch (SQLException e) {
			System.out.println();
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		System.out.println("*** Completed!!");
	}

	private void updatePlayerClusteringIndex(PreparedStatement ps,
			TrustNode player, int serverId, String monthRange)
			throws SQLException {

		init();
		int numTriangles = findTriangles(player);
		int numConnectedTriples = findAllTriples(player);
		if (numConnectedTriples != 0) {
			// System.out.println("Updating for char: " +
			// player.getCharacterId());
			float clusteringIndex = (float) (3 * numTriangles)
					/ numConnectedTriples;
			executeUpdate(ps, clusteringIndex, serverId, monthRange,
					player.getCharacterId());
		}
	}

	private static Set<String> TRIANGLE_SET = new HashSet<String>();
	private static Set<String> TRIPLE_SET = new HashSet<String>();

	private void init() {

		TRIANGLE_SET = new HashSet<String>();
		TRIPLE_SET = new HashSet<String>();
	}

	private int findTriangles(TrustNode player) {

		List<Long> neighborsOne = player.getNeighborCharacters();
		if (neighborsOne != null && neighborsOne.size() > 0) {
			for (int i = 0; i < neighborsOne.size(); i++) {

				TrustNode neighborOne = GLOBAL_PLAYER_MAP.get(neighborsOne
						.get(i));
				List<Long> neighborsTwo = neighborOne.getNeighborCharacters();
				if (neighborsTwo != null && neighborsTwo.size() > 0) {
					for (int j = 0; j < neighborsTwo.size(); j++) {

						TrustNode neighborTwo = GLOBAL_PLAYER_MAP
								.get(neighborsTwo.get(j));
						if (neighborTwo.getCharacterId() == player
								.getCharacterId())
							continue;
						List<Long> neighborsThree = neighborTwo
								.getNeighborCharacters();
						if (neighborsThree != null && neighborsThree.size() > 0) {
							for (int k = 0; k < neighborsThree.size(); k++) {

								TrustNode neighborThree = GLOBAL_PLAYER_MAP
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

	private void addToTriangleSet(Long one, Long two, Long three) {

		List<Long> triangle = new ArrayList<Long>();
		triangle.add(one);
		triangle.add(two);
		triangle.add(three);
		Collections.sort(triangle);
		String key = triangle.get(0) + "_" + triangle.get(1) + "_"
				+ triangle.get(2);
		if (!TRIANGLE_SET.contains(key)) {
			// System.out.println("triangle " + key);
			TRIANGLE_SET.add(key);
		}
	}

	private int findAllTriples(TrustNode player) {

		findOriginTriples(player);
		findCentredTriples(player);

		return TRIPLE_SET.size();
	}

	private void findOriginTriples(TrustNode player) {

		List<Long> neighborsOne = player.getNeighborCharacters();
		if (neighborsOne != null && neighborsOne.size() > 0) {
			for (int i = 0; i < neighborsOne.size(); i++) {

				TrustNode neighborOne = GLOBAL_PLAYER_MAP.get(neighborsOne
						.get(i));
				List<Long> neighborsTwo = neighborOne.getNeighborCharacters();
				if (neighborsTwo != null && neighborsTwo.size() > 0) {
					for (int j = 0; j < neighborsTwo.size(); j++) {

						TrustNode neighborTwo = GLOBAL_PLAYER_MAP
								.get(neighborsTwo.get(j));
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

	private void findCentredTriples(TrustNode player) {

		List<Long> neighbors = player.getNeighborCharacters();
		// 1 because at least two neighbors are required to form a
		// centred-triple
		if (neighbors != null && neighbors.size() > 1) {
			// for enumeration
			Collections.sort(neighbors);
			for (int i = 0; i < neighbors.size(); i++) {
				Long neighborOne = neighbors.get(i);
				for (int j = i + 1; j < neighbors.size(); j++) {
					Long neighborTwo = neighbors.get(j);
					addToTripleSet(neighborOne, player.getCharacterId(),
							neighborTwo, false);
				}
			}
		}
	}

	private void addToTripleSet(Long one, Long two, Long three,
			boolean isOneAtOrigin) {

		List<Long> triple = new ArrayList<Long>();
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

				List<Long> triple1 = new ArrayList<Long>();
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

	public void loadPlayerDegreeCentrality(LPNetwork network,
			String monthRange, int serverId) {

		System.out.println("*** Loading raw player network into graph...");
		loadJUNGGraph();
		System.out
				.println("*** Computing degree centrality for all player nodes...");
		Connection conn = getDestConn();
		try {
			PreparedStatement stmt_update = getUpdateStatement(conn, network,
					"degree_cent");
			Collection<Long> vertices = JUNG_GRAPH.getVertices();
			int denom = vertices.size() - 1;
			for (Long vertex : vertices) {
				float degreeCentrality = (float) JUNG_GRAPH
						.getNeighborCount(vertex) / denom;
				executeUpdate(stmt_update, degreeCentrality, serverId,
						monthRange, vertex);
				conn.commit();
			}
		} catch (SQLException e) {
			System.out.println();
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		System.out.println("*** Completed!!");
	}

	public void loadPlayerBetweennessCentrality(LPNetwork network,
			String monthRange, int serverId) {

		System.out.println("*** Loading raw player network into graph...");
		loadJUNGGraph();
		System.out
				.println("*** Computing betweenness centrality for all player nodes...");
		Connection conn = getDestConn();
		try {

			BetweennessCentrality<Long, GraphLink> ranker = new BetweennessCentrality(
					JUNG_GRAPH);
			PreparedStatement stmt_update = getUpdateStatement(conn, network,
					"betweenness_cent");
			Collection<Long> vertices = JUNG_GRAPH.getVertices();
			int n = vertices.size();
			float normalizingConstant = ((float) (n - 1) * (n - 2)) / 2;
			for (Long vertex : vertices) {
				double rankScore = ranker.getVertexScore(vertex);
				float normScore = (float) rankScore / normalizingConstant;
				executeUpdate(stmt_update, normScore, serverId, monthRange,
						vertex);
				conn.commit();
			}
		} catch (SQLException e) {
			System.out.println();
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		System.out.println("*** Completed!!");
	}

	public void loadPlayerClosenessCentrality(LPNetwork network,
			String monthRange, int serverId) {

		System.out.println("*** Loading raw player network into graph...");
		loadJUNGGraph();
		System.out
				.println("*** Computing closeness centrality for all player nodes...");
		Connection conn = getDestConn();
		try {

			ClosenessCentrality<Long, GraphLink> ranker = new ClosenessCentrality(
					JUNG_GRAPH);
			PreparedStatement stmt_update = getUpdateStatement(conn, network,
					"closeness_cent");
			Collection<Long> vertices = JUNG_GRAPH.getVertices();
			for (Long vertex : vertices) {
				double rankScore = ranker.getVertexScore(vertex);
				executeUpdate(stmt_update, (float) rankScore, serverId,
						monthRange, vertex);
				conn.commit();
			}
		} catch (SQLException e) {
			System.out.println();
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		System.out.println("*** Completed!!");
	}

	public void loadPlayerEigenvectorCentrality(LPNetwork network,
			String monthRange, int serverId) {

		System.out.println("*** Loading raw player network into graph...");
		loadJUNGGraph();
		System.out
				.println("*** Computing eigenvector centrality for all player nodes...");
		Connection conn = getDestConn();
		try {

			EigenvectorCentrality<Long, GraphLink> ranker = new EigenvectorCentrality(
					JUNG_GRAPH);
			PreparedStatement stmt_update = getUpdateStatement(conn, network,
					"eigenvector_cent");
			Collection<Long> vertices = JUNG_GRAPH.getVertices();
			for (Long vertex : vertices) {
				double rankScore = ranker.getVertexScore(vertex);
				executeUpdate(stmt_update, (float) rankScore, serverId,
						monthRange, vertex);
				conn.commit();
			}
		} catch (SQLException e) {
			System.out.println();
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		System.out.println("*** Completed!!");
	}

	private void loadJUNGGraph() {

		JUNG_GRAPH = new UndirectedSparseGraph<Long, GraphLink>();
		loadPlayersMap();
		int edgeNum = 0;
		for (TrustNode player : GLOBAL_PLAYER_MAP.values()) {
			long characterId = player.getCharacterId();
			JUNG_GRAPH.addVertex(characterId);
			List<Long> neighbors = player.getNeighborCharacters();
			if (neighbors != null && neighbors.size() > 0) {
				for (Long neighbor : neighbors) {
					long edgeKey = GraphLink.constructKey(characterId,
							(long) neighbor);
					GraphLink graphLink = new GraphLink(edgeKey, 1, 1);
					if (!JUNG_GRAPH.containsEdge(graphLink)) {
						graphLink.setEdgeNum(++edgeNum);
						JUNG_GRAPH.addEdge(graphLink, characterId,
								(long) neighbor);
					}
				}
			}
		}
	}

	private void loadPlayersMap() {

		GLOBAL_PLAYER_MAP = new HashMap<Long, TrustNode>();
		Connection conn = getSourceConn();
		try {

			Statement stmt = conn.createStatement();
			System.out.println("** Start: fetch data");
			String query = "select * from lp_training_period_edge;";
			System.out.println("Query: " + query);
			ResultSet rs = stmt.executeQuery(query);
			System.out.println("** End: fetch data");
			while (rs.next()) {
				long charOneId = rs.getLong("char1_id");
				TrustNode playerOne = getPlayer(charOneId);
				long charTwoId = rs.getLong("char2_id");
				TrustNode playerTwo = getPlayer(charTwoId);

				playerOne.addToNeighborCharacters(charTwoId);
				playerTwo.addToNeighborCharacters(charOneId);
				// System.out.println("cnt = " + ++count + "charOneId = "
				// + charOneId);
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

	private TrustNode getPlayer(long charId) {
		TrustNode player = null;
		if (GLOBAL_PLAYER_MAP.containsKey(charId)) {
			player = GLOBAL_PLAYER_MAP.get(charId);
		} else {
			player = new TrustNode(charId);
			GLOBAL_PLAYER_MAP.put(charId, player);
		}

		return player;
	}

	private PreparedStatement getUpdateStatement(Connection conn,
			LPNetwork network, String featureSuffix) throws SQLException {

		String updateStmt = "update lp_player_tp_features set "
				+ network.getFeaturePrefix() + "_" + featureSuffix
				+ "= ? where server_id = ? and month_range = ? and char_id = ?";
		System.out.println("Update statement: " + updateStmt);
		return conn.prepareStatement(updateStmt);
	}

	private void executeUpdate(PreparedStatement ps, float featureValue,
			int serverId, String monthRange, long charId) throws SQLException {

		ps.setFloat(1, featureValue);
		ps.setInt(2, serverId);
		ps.setString(3, monthRange);
		ps.setLong(4, charId);
		ps.executeUpdate();
	}

	private String getCommaSepCharacters(List<Long> neighbors) {

		StringBuilder str = new StringBuilder();
		str.append(String.valueOf(neighbors.get(0)));
		for (int i = 1; i < neighbors.size(); i++) {
			str.append(",").append(String.valueOf(neighbors.get(i)));
		}

		return str.toString();
	}

	private Connection getSourceConn() {
		return ConnectionUtil.getGUILETrustConnection(false);
	}

	private Connection getDestConn() {
		return ConnectionUtil.getGUILETrustConnection(false);
	}

	private void nullSafeSet(PreparedStatement statement, int index,
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
}
