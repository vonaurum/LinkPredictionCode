/**
 * 
 */
package linkpred.superlearn.generic;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import util.ConnectionUtil;

import linkpred.superlearn.bean.Player;

/**
 * @author Zoheb H Borbora
 * 
 */
public abstract class AbstractDataProcessor {

	protected Map<Integer, Player> GLOBAL_PLAYER_MAP = new HashMap<Integer, Player>();
	protected int COUNT_EDGE = 0;
	protected int COUNT_PLAYER = 0;
	protected int COUNT_EDGE_SAMPLE = 0;
	protected static String POSITIVE_LABEL = "Y";
	protected static String NEGATIVE_LABEL = "N";

	protected abstract void deleteExistingPlayerEdges();

	protected abstract ResultSet getRelevantPairsData(Connection conn)
			throws SQLException;

	protected abstract Long getCharOneId(ResultSet rs) throws SQLException;

	protected abstract Long getCharTwoId(ResultSet rs) throws SQLException;

	protected abstract void insertWeightedEdge(Connection conn,
			int smallerCharId, int biggerCharId, int weight)
			throws SQLException;

	public void rebuildPlayerEdges() {
		deleteExistingPlayerEdges();
		System.out.println("Deleted existing edges.");
		buildPlayerEdges();
		System.out.println("No. of edges = " + COUNT_EDGE);
	}

	protected void buildPlayerEdges() {

		Map<String, Integer> weightedEdgeMap = new HashMap<String, Integer>();
		Connection conn = ConnectionUtil.getGUILEConnection(false);
		try {

			System.out.println("Getting Constructing edge + weights.. ");
			ResultSet rs = getRelevantPairsData(conn);
			System.out.println("Constructing edge + weights.. ");
			while (rs.next()) {

				try {

					int charOneId = (int) (long) getCharOneId(rs);
					int charTwoId = (int) (long) getCharTwoId(rs);

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

			// System.out.println("Edge map size = " + weightedEdgeMap.size());
			for (Map.Entry<String, Integer> mapEntry : weightedEdgeMap
					.entrySet()) {

				String chrs = mapEntry.getKey();
				String[] tokens = chrs.split("_");
				insertWeightedEdge(conn, Integer.valueOf(tokens[0]),
						Integer.valueOf(tokens[1]), mapEntry.getValue());
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

	private String getEdgeKey(int charOneId, int charTwoId) {

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
}