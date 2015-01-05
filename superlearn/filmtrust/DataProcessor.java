package linkpred.superlearn.filmtrust;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import util.ConnectionUtil;
import util.MathUtil;

public class DataProcessor {

	private String POSITIVE_LABEL = "Y";
	private String NEGATIVE_LABEL = "N";

	private String INPUT_FILE = "C:\\Users\\zoheb\\work\\eclipse_workspace\\linkpred\\src\\linkpred\\superlearn\\filmtrust\\adjListJan18Crypt.csv";

	private int NUM_TOTAL_POSITIVE_EDGES = 1290;

	private int NUM_TRAINING_POSITIVE_EDGES = 1000;

	private int NUM_TRAINING_NEGATIVE_EDGES = 2000;

	private int NUM_TEST_NEGATIVE_EDGES = 600;

	private Set<String> FT_TRAINING_NODES = new HashSet<String>();

	private Set<String> FT_TEST_NODES = new HashSet<String>();

	private Set<String> FT_TRAINING_EDGES = new HashSet<String>();

	private Set<String> FT_TEST_EDGES = new HashSet<String>();

	private static String SQL_INSERT_TRAINING_EDGE = "INSERT INTO ft_training_edge("
			+ "ft_training_edge_id, char1_id, char2_id, form_link) VALUES (?,?,?,?)";

	private static String SQL_INSERT_TEST_EDGE = "INSERT INTO ft_test_edge("
			+ "ft_test_edge_id, char1_id, char2_id, form_link) VALUES (?,?,?,?)";

	private void insertTrainingEdge(Connection conn, String charOneId,
			String charTwoId, String label) throws SQLException {

		PreparedStatement ps = conn.prepareStatement(SQL_INSERT_TRAINING_EDGE);
		ps.setLong(1, getMaxTrainingId(conn) + 1);
		ps.setString(2, charOneId);
		ps.setString(3, charTwoId);
		ps.setString(4, label);
		ps.executeUpdate();
	}

	private void insertTestEdge(Connection conn, String charOneId,
			String charTwoId, String label) throws SQLException {

		PreparedStatement ps = conn.prepareStatement(SQL_INSERT_TEST_EDGE);
		ps.setLong(1, getMaxTestId(conn) + 1);
		ps.setString(2, charOneId);
		ps.setString(3, charTwoId);
		ps.setString(4, label);
		ps.executeUpdate();
	}

	public void rebuildEdgeSamples() {
		deleteExistingEdgeSamples();
		System.out.println("Deleted all existing samples");
		buildPositiveEdgeSamples();
		System.out.println("Inserted positive samples");
		buildNegativeEdgeSamples();
		System.out.println("Inserted negative samples");
	}

	private void buildPositiveEdgeSamples() {

		Connection conn = ConnectionUtil.getLUIGIConnection(false);
		BufferedReader reader;
		try {

			Set<Integer> trainingEdgeIdxSet = MathUtil.getRandomSample(1,
					NUM_TOTAL_POSITIVE_EDGES, NUM_TRAINING_POSITIVE_EDGES);
			reader = new BufferedReader(new FileReader(INPUT_FILE));
			String line = reader.readLine();
			int lineCount = 1;
			while (line != null) {
				String[] tokens = line.split(",");
				String char1 = tokens[0];
				String char2 = tokens[1];

				if (trainingEdgeIdxSet.contains(lineCount)) {
					FT_TRAINING_EDGES.add(char1 + "_" + char2);
					FT_TRAINING_NODES.add(char1);
					FT_TRAINING_NODES.add(char2);
					insertTrainingEdge(conn, char1, char2, POSITIVE_LABEL);
				} else {
					FT_TEST_EDGES.add(char1 + "_" + char2);
					FT_TEST_NODES.add(char1);
					FT_TEST_NODES.add(char2);
					insertTestEdge(conn, char1, char2, POSITIVE_LABEL);
				}

				line = reader.readLine();
				lineCount++;
			}
			conn.commit();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
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

	private void buildNegativeEdgeSamples() {

		Connection conn = ConnectionUtil.getLUIGIConnection(false);
		try {

			List<String> nodeList = Arrays.asList(FT_TRAINING_NODES
					.toArray(new String[FT_TRAINING_NODES.size()]));
			int insertionCount = 0;
			while (insertionCount < NUM_TRAINING_NEGATIVE_EDGES) {
				List<String> edge = getNegativeEdge(nodeList, FT_TRAINING_EDGES);
				insertTrainingEdge(conn, edge.get(0), edge.get(1),
						NEGATIVE_LABEL);
				insertionCount++;
			}

			nodeList = Arrays.asList(FT_TEST_NODES
					.toArray(new String[FT_TEST_NODES.size()]));
			insertionCount = 0;
			while (insertionCount < NUM_TEST_NEGATIVE_EDGES) {
				List<String> edge = getNegativeEdge(nodeList, FT_TEST_EDGES);
				insertTestEdge(conn, edge.get(0), edge.get(1), NEGATIVE_LABEL);
				insertionCount++;
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

	private List<String> getNegativeEdge(List<String> nodeList,
			Set<String> edgeExclusionSet) {

		List<String> ret = new ArrayList();
		while (true) {
			// get char ids of two individual training players at random
			int random = MathUtil.getRandomNumber(0, nodeList.size() - 1);
			String c1 = nodeList.get(random);

			random = MathUtil.getRandomNumber(0, nodeList.size() - 1);
			String c2 = nodeList.get(random);

			// make sure the pair does not ever form a link
			if (!(edgeExclusionSet.contains(c1 + "_" + c2) || edgeExclusionSet
					.contains(c2 + "_" + c1))) {
				ret.add(c1);
				ret.add(c2);
				break;
			}
		}

		return ret;
	}

	private void deleteExistingEdgeSamples() {

		Connection conn = ConnectionUtil.getLUIGIConnection(false);
		try {
			Statement stmt = conn.createStatement();
			stmt.executeUpdate("delete from ft_training_edge");
			stmt.executeUpdate("delete from ft_test_edge");
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

	private long getMaxTrainingId(Connection conn) throws SQLException {

		Statement stmt = conn.createStatement();
		ResultSet rs = stmt
				.executeQuery("select MAX(ft_training_edge_id) AS max_num from ft_training_edge");

		return (rs != null && rs.next()) ? rs.getLong("max_num") : 0;
	}

	private long getMaxTestId(Connection conn) throws SQLException {

		Statement stmt = conn.createStatement();
		ResultSet rs = stmt
				.executeQuery("select MAX(ft_test_edge_id) AS max_num from ft_test_edge");

		return (rs != null && rs.next()) ? rs.getLong("max_num") : 0;
	}
}
