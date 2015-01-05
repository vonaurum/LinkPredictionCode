package linkpred.superlearn.generic;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import util.ConnectionUtil;

public class TestDataProcessor extends AbstractDataProcessor {

	// TODO: write this query for each set
	protected ResultSet getRelevantPairsData(Connection conn)
			throws SQLException {

		Statement stmt = conn.createStatement();
		return stmt.executeQuery("select * from group_graph_jun");
	}

	protected Long getCharOneId(ResultSet rs) throws SQLException {

		return rs.getLong("src_char_id");
	}

	protected Long getCharTwoId(ResultSet rs) throws SQLException {

		return rs.getLong("dest_char_id");
	}

	protected void deleteExistingPlayerEdges() {

		COUNT_EDGE = 0;
		Connection conn = ConnectionUtil.getGUILEConnection(false);
		try {
			Statement stmt = conn.createStatement();
			stmt.executeUpdate("delete from test_period_edge");
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

	private static String SQL_INSERT_WEIGHTED_EDGE = "INSERT INTO "
			+ "test_period_edge(test_period_edge_id, "
			+ "char1_id, char2_id, num_count) VALUES (?, ?, ?, ?)";

	protected void insertWeightedEdge(Connection conn, int charOneId,
			int charTwoId, int weight) throws SQLException {

		PreparedStatement ps = conn.prepareStatement(SQL_INSERT_WEIGHTED_EDGE);
		ps.setInt(1, ++COUNT_EDGE);
		ps.setLong(2, charOneId);
		ps.setLong(3, charTwoId);
		ps.setInt(4, weight);
		ps.executeUpdate();
		// System.out.println("1 - Inserted " + COUNT_EDGE + " - " + charOneId);
	}
}
