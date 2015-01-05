/**
 * 
 */
package linkpred.superlearn.bbn;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;

import network.GraphLink;
import util.ConnectionUtil;
import edu.uci.ics.jung.algorithms.scoring.BetweennessCentrality;
import edu.uci.ics.jung.algorithms.scoring.ClosenessCentrality;
import edu.uci.ics.jung.algorithms.scoring.EigenvectorCentrality;
import edu.uci.ics.jung.graph.UndirectedGraph;
import edu.uci.ics.jung.graph.UndirectedSparseGraph;

/**
 * @author Zoheb H Borbora
 */
public class BBNExtFeatureSetConstructor extends BBNFeatureSetConstructor {

	UndirectedGraph<Integer, GraphLink> JUNG_GRAPH = new UndirectedSparseGraph<Integer, GraphLink>();

	public void loadJUNGGraph() throws SQLException {

		JUNG_GRAPH = new UndirectedSparseGraph<Integer, GraphLink>();
		Connection conn = ConnectionUtil.getGUILEConnection(false);
		try {

			System.out.println("Loading player graph..");
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt
					.executeQuery("SELECT char_id, account, neighbors FROM bbn_training_period_player");
			int edgeNum = 0;
			while (rs.next()) {

				int characterId = rs.getInt("char_id");
				JUNG_GRAPH.addVertex(characterId);
				List<Integer> neighbors = getCharacterList(rs
						.getString("neighbors"));
				if (neighbors != null && neighbors.size() > 0) {
					for (Integer neighbor : neighbors) {

						long edgeKey = GraphLink.constructKey(
								(long) characterId, (long) neighbor);
						GraphLink graphLink = new GraphLink(edgeKey, 1, 1);
						if (!JUNG_GRAPH.containsEdge(graphLink)) {
							graphLink.setEdgeNum(++edgeNum);
							JUNG_GRAPH
									.addEdge(graphLink, characterId, neighbor);
						}
					}
				}
			}
			System.out.println("Completed loading player graph");

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

	public void computePlayerDegreeCentrality() {

		Connection conn = ConnectionUtil.getGUILEConnection(false);
		try {

			Collection<Integer> vertices = JUNG_GRAPH.getVertices();
			int denom = vertices.size() - 1;
			for (Integer vertex : vertices) {

				float degreeCentrality = (float) JUNG_GRAPH
						.getNeighborCount(vertex) / denom;
				PreparedStatement ps = conn
						.prepareStatement("UPDATE bbn_training_period_player SET degree_cent = ? WHERE char_id = ?;");
				ps.setFloat(1, degreeCentrality);
				ps.setInt(2, vertex);
				ps.executeUpdate();
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

	public void computePlayerBetweennessCentrality() {

		Connection conn = ConnectionUtil.getGUILEConnection(false);
		try {

			BetweennessCentrality<Integer, GraphLink> ranker = new BetweennessCentrality(
					JUNG_GRAPH);
			Collection<Integer> vertices = JUNG_GRAPH.getVertices();
			int n = vertices.size();
			float normalizingConstant = ((float) (n - 1) * (n - 2)) / 2;
			for (Integer vertex : vertices) {
				double rankScore = ranker.getVertexScore(vertex);
				float normScore = (float) rankScore / normalizingConstant;
				PreparedStatement ps = conn
						.prepareStatement("UPDATE bbn_training_period_player SET betweenness_cent = ? WHERE char_id = ?;");
				ps.setFloat(1, normScore);
				ps.setInt(2, vertex);
				ps.executeUpdate();
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

	public void computePlayerClosenessCentrality() {

		Connection conn = ConnectionUtil.getGUILEConnection(false);
		try {

			ClosenessCentrality<Integer, GraphLink> ranker = new ClosenessCentrality(
					JUNG_GRAPH);
			Collection<Integer> vertices = JUNG_GRAPH.getVertices();
			for (Integer vertex : vertices) {
				double rankScore = ranker.getVertexScore(vertex);
				PreparedStatement ps = conn
						.prepareStatement("UPDATE bbn_training_period_player SET closeness_cent = ? WHERE char_id = ?;");
				ps.setFloat(1, (float) rankScore);
				ps.setInt(2, vertex);
				ps.executeUpdate();
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

	public void computePlayerEigenvectorCentrality() {

		Connection conn = ConnectionUtil.getGUILEConnection(false);
		try {

			EigenvectorCentrality<Integer, GraphLink> ranker = new EigenvectorCentrality(
					JUNG_GRAPH);
			Collection<Integer> vertices = JUNG_GRAPH.getVertices();
			for (Integer vertex : vertices) {
				double rankScore = ranker.getVertexScore(vertex);
				// System.out.println("vertex = " + vertex + " rankScore = " +
				// rankScore);
				PreparedStatement ps = conn
						.prepareStatement("UPDATE bbn_training_period_player SET eigenvector_cent = ? WHERE char_id = ?;");
				ps.setFloat(1, (float) rankScore);
				ps.setInt(2, vertex);
				ps.executeUpdate();
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
}
