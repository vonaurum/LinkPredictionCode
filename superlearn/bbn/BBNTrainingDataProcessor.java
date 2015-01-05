package linkpred.superlearn.bbn;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import util.ConnectionUtil;
import util.MathUtil;

import linkpred.superlearn.bean.BBNNode;

public class BBNTrainingDataProcessor extends BBNAbstractDataProcessor {

	protected ResultSet getRelevantPairsData(Connection conn)
			throws SQLException {

		Statement stmt = conn.createStatement();
		return stmt.executeQuery("select * from trust_net_guk_jan_jun");
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
			stmt.executeUpdate("delete from bbn_training_period_edge");
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
			+ "bbn_training_period_edge(training_period_edge_id, "
			+ "char1_id, char2_id, num_count) VALUES (?, ?, ?, ?)";

	protected void insertWeightedEdge(Connection conn, int charOneId,
			int charTwoId, int weight) throws SQLException {

		PreparedStatement ps = conn.prepareStatement(SQL_INSERT_WEIGHTED_EDGE);
		ps.setInt(1, ++COUNT_EDGE);
		ps.setLong(2, charOneId);
		ps.setLong(3, charTwoId);
		ps.setInt(4, weight);
		ps.executeUpdate();
		// System.out.println("2 - Inserted " + COUNT_EDGE + " - " + charOneId);
	}

	private static String SQL_FETCH_GUK_CHARACTER_INFO = "SELECT c.account, c.class_id, "
			+ "c.char_level, c.gender, c.race FROM char_store_min c "
			+ "WHERE server_id = 101 AND c.character_id = ";

	private static String SQL_FETCH_DEMOGRAPHICS_INFO = "SELECT d.dob, d.join_date, "
			+ "d.country, d.sex FROM demographics d WHERE D.account = ";

	private static String SQL_FETCH_GUILDS_INFO = "select character_id, guild_id from eq2_character_store where server_id = 101";

	private BBNNode getPlayer(int charId) {

		BBNNode player = null;
		if (GLOBAL_PLAYER_MAP.containsKey(charId)) {
			player = GLOBAL_PLAYER_MAP.get(charId);
		} else {
			player = new BBNNode(charId);
			GLOBAL_PLAYER_MAP.put(charId, player);
		}

		return player;
	}

	public void rebuildPlayerData() {

		System.out.println("Loading all players into memory...");
		loadPlayersMap();
		System.out.println("No. of players = " + GLOBAL_PLAYER_MAP.size());
		deleteAllPlayerData();
		System.out.println("Deleted existing player data.");
		System.out.println("Re-populating player data....");
		buildAllPlayerData();
		System.out.println("Populated player data. No. of players = "
				+ COUNT_PLAYER);
		GLOBAL_PLAYER_MAP.clear();
	}

	private void loadPlayersMap() {

		GLOBAL_PLAYER_MAP = new HashMap<Integer, BBNNode>();
		Connection conn = ConnectionUtil.getGUILEConnection(false);
		try {

			Map<Integer, Integer> guildIdMap = loadGuildIdMap();
			int count = 0;
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt
					.executeQuery("select * from bbn_training_period_edge;");
			while (rs.next()) {

				int charOneId = rs.getInt("char1_id");
				BBNNode playerOne = getPlayer(charOneId);
				putCharacterInfo(conn, playerOne);
				putDemographicsInfo(conn, playerOne);

				int charTwoId = rs.getInt("char2_id");
				BBNNode playerTwo = getPlayer(charTwoId);
				putCharacterInfo(conn, playerTwo);
				putDemographicsInfo(conn, playerTwo);

				playerOne.setGuildId(guildIdMap.get(charOneId));
				playerTwo.setGuildId(guildIdMap.get(charTwoId));

				playerOne.addToNeighborCharacters(charTwoId);
				playerTwo.addToNeighborCharacters(charOneId);

				System.out.println("cnt = " + ++count + "charOneId = "
						+ charOneId);
			}
			System.out.println("Finished loading players map.");

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

	private Map<Integer, Integer> loadGuildIdMap() {

		Map<Integer, Integer> map = new HashMap<Integer, Integer>();
		Connection conn = ConnectionUtil.getNCSAConnection();
		try {

			System.out.println("Loading guilds map.. ");
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(SQL_FETCH_GUILDS_INFO);
			while (rs.next()) {
				Integer charId = rs.getInt("character_id");
				Object guildId = rs.getObject("guild_id");
				if (guildId != null) {
					map.put(charId, Integer.valueOf((String) guildId));
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

		return map;
	}

	private void putCharacterInfo(Connection conn, BBNNode player) {

		if (player.isCharInfoFetched())
			return;

		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(SQL_FETCH_GUK_CHARACTER_INFO
					+ player.getCharacterId() + ";");
			player.setCharInfoFetched(true);
			if (rs != null && rs.next()) {
				player.setAccountId(rs.getInt("account"));
				player.setCharClassId(rs.getInt("class_id"));
				player.setCharLevel(rs.getInt("char_level"));
				player.setCharGender(rs.getInt("gender"));
				player.setCharRace(rs.getInt("race"));
			}

			if (rs.next()) {
				System.out
						.println("****** Alert1: more than one row retrieved for guk and charId = "
								+ player.getCharacterId());
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private void putDemographicsInfo(Connection conn, BBNNode player) {

		if (player.isDemographicsInfoFetched())
			return;

		try {

			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(SQL_FETCH_DEMOGRAPHICS_INFO
					+ player.getAccountId() + ";");
			player.setDemographicsInfoFetched(true);
			if (rs != null && rs.next()) {
				player.setCountry(rs.getString("country"));
				player.setRealGender(rs.getString("sex"));
				player.setAge2006(calculateAgeAt2006(rs.getString("dob")));
				player.setAgeAtJoining(calculateAgeAtjoining(
						rs.getString("join_date"), rs.getString("dob")));
			}
			if (rs.next()) {
				System.out
						.println("****** Alert2: more than one row retrieved for accountId = "
								+ player.getAccountId());
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private int calculateAgeAt2006(String strDate) {

		int ret = -99;

		if (strDate != null && !strDate.trim().equals("")) {

			DateFormat format = new SimpleDateFormat("dd-MMM-yy");
			try {
				Date dob = format.parse(strDate);
				Calendar cal = Calendar.getInstance();
				cal.setTime(dob);
				ret = 2006 - cal.get(Calendar.YEAR);
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}

		return ret;
	}

	private int calculateAgeAtjoining(String strJoinDate, String strDob) {

		int ret = -99;

		if (strJoinDate != null && !strJoinDate.trim().equals("")) {

			DateFormat format = new SimpleDateFormat("dd-MMM-yy");
			try {

				Date joinDate = format.parse(strJoinDate);
				Calendar calJoin = Calendar.getInstance();
				calJoin.setTime(joinDate);

				Date dob = format.parse(strDob);
				Calendar calDob = Calendar.getInstance();
				calDob.setTime(dob);

				ret = calJoin.get(Calendar.YEAR) - calDob.get(Calendar.YEAR);
				if ((calJoin.get(Calendar.MONTH) - calDob.get(Calendar.MONTH)) > 0) {
					ret += 1;
				}

			} catch (ParseException e) {
				e.printStackTrace();
			}
		}

		return ret;
	}

	private void deleteAllPlayerData() {

		COUNT_PLAYER = 0;
		Connection conn = ConnectionUtil.getGUILEConnection(false);
		try {
			Statement stmt = conn.createStatement();
			stmt.executeUpdate("delete from bbn_training_period_player");

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

	private void buildAllPlayerData() {

		Connection conn = ConnectionUtil.getGUILEConnection(false);
		try {
			for (BBNNode player : GLOBAL_PLAYER_MAP.values()) {
				insertPlayer(conn, player);
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
	}

	private void insertPlayer(Connection conn, BBNNode player) {

		PreparedStatement ps;
		try {
			ps = conn
					.prepareStatement("INSERT INTO bbn_training_period_player"
							+ "(player_id, char_id, account, access_level, "
							+ "real_gender, country, age_2006, age_joining, char_class_id, char_level, "
							+ "char_gender, char_race, num_items_moved, num_items_pickup, "
							+ "num_items_placed, neighbors, guild_id) "
							+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

			ps.setInt(1, ++COUNT_PLAYER);
			ps.setInt(2, player.getCharacterId());
			nullSafeSet(ps, 3, player.getAccountId(), Integer.class);
			nullSafeSet(ps, 4, player.getAccessLevel(), String.class);
			nullSafeSet(ps, 5, player.getRealGender(), String.class);
			nullSafeSet(ps, 6, player.getCountry(), String.class);
			nullSafeSet(ps, 7, player.getAge2006(), Integer.class);
			nullSafeSet(ps, 8, player.getAgeAtJoining(), Integer.class);
			nullSafeSet(ps, 9, player.getCharClassId(), Integer.class);
			nullSafeSet(ps, 10, player.getCharLevel(), Integer.class);
			nullSafeSet(ps, 11, player.getCharGender(), Integer.class);
			nullSafeSet(ps, 12, player.getCharRace(), Integer.class);
			nullSafeSet(ps, 13, player.getNumItemsMoved(), Integer.class);
			nullSafeSet(ps, 14, player.getNumItemsPickup(), Integer.class);
			nullSafeSet(ps, 15, player.getNumItemsPlaced(), Integer.class);
			nullSafeSet(ps, 16,
					getCommaSepCharacters(player.getNeighborCharacters()),
					String.class);
			nullSafeSet(ps, 17, player.getGuildId(), Integer.class);

			ps.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		// System.out.println("3 - Inserted " + COUNT_PLAYER + " - "
		// + player.getCharacterId());
	}

	private String getCommaSepCharacters(List<Integer> neighbors) {

		StringBuilder str = new StringBuilder();
		str.append(String.valueOf(neighbors.get(0)));
		for (int i = 1; i < neighbors.size(); i++) {
			str.append(",").append(String.valueOf(neighbors.get(i)));
		}

		return str.toString();
	}

	public int buildPositiveEdgeSamples(int maxSampleSize) {

		Connection conn = ConnectionUtil.getGUILEConnection(false);
		try {

			Statement stmt = conn.createStatement();

			/*
			 * build an exclusion set of player-pairs in test who also appear in
			 * training period
			 */
			ResultSet rsOne = stmt
					.executeQuery("SELECT t2.char1_id, t2.char2_id "
							+ "FROM bbn_training_period_edge t1, bbn_test_period_edge t2 "
							+ "WHERE t1.char1_id = t2.char1_id "
							+ "AND t1.char2_id = t2.char2_id");
			Set<String> exclusionSet = new HashSet<String>();
			while (rsOne.next()) {
				exclusionSet.add(rsOne.getInt("char1_id") + "_"
						+ rsOne.getInt("char2_id"));
			}

			// get all individual players present in training period
			Set<Integer> trainingPlayersSet = getTrainingPlayerAccounts(conn);

			// get all edges from test period
			ResultSet rsTwo = stmt
					.executeQuery("SELECT t.char1_id, t.char2_id FROM bbn_test_period_edge t");

			while (rsTwo.next()) {
				int charOne = rsTwo.getInt("char1_id");
				int charTwo = rsTwo.getInt("char2_id");

				/*
				 * process only if test player-pair is not in exclusion set i.e
				 * the pair is not present in training period
				 */
				if (!exclusionSet.contains(charOne + "_" + charTwo)) {

					// make sure each individual player is present during
					// training period
					if (trainingPlayersSet.contains(charOne)
							&& trainingPlayersSet.contains(charTwo)) {

						// we have a positive sample!!
						insertEdgeSample(conn, COUNT_EDGE_SAMPLE++, charOne,
								charTwo, POSITIVE_LABEL);
					} else {
						/*
						 * System.out.println("Not present in training : " +
						 * accountOne + " " + accountTwo);
						 */
					}
				}

				if (COUNT_EDGE_SAMPLE > maxSampleSize)
					break;
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

		return COUNT_EDGE_SAMPLE;
	}

	public int buildNegativeEdgeSamples(int maxTotalSampleSize) {

		Connection conn = ConnectionUtil.getGUILEConnection(false);
		try {

			int pairId = 30000;
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT max(player_pairs_id) "
					+ "FROM bbn_training_period_pairs t ");
			if (rs != null && rs.next()) {
				pairId = rs.getInt(1) + 1;
			}

			// get all individual players present in training period
			Set<Integer> trainingPlayersSet = getTrainingPlayerAccounts(conn);
			/*
			 * build an exclusion set of player-pairs from both training and
			 * test period - such pairs cannot be a negative sample
			 */
			ResultSet rsOne = stmt
					.executeQuery("SELECT t1.char1_id, t1.char2_id "
							+ "FROM bbn_training_period_edge t1 UNION "
							+ "SELECT t2.char1_id, t2.char2_id "
							+ "FROM bbn_test_period_edge t2;");
			Set<String> exclusionSet = new HashSet<String>();
			while (rsOne.next()) {
				exclusionSet.add(rsOne.getInt("char1_id") + "_"
						+ rsOne.getInt("char2_id"));
			}

			List<Integer> trainingPlayersList = new ArrayList<Integer>(
					trainingPlayersSet);
			int listSize = trainingPlayersList.size();

			COUNT_EDGE_SAMPLE = 0;
			while (pairId < maxTotalSampleSize) {

				// get account ids of two individual training players at random
				int random = MathUtil.getRandomNumber(0, listSize - 1);
				int t1 = trainingPlayersList.get(random);

				random = MathUtil.getRandomNumber(0, listSize - 1);
				int t2 = trainingPlayersList.get(random);

				// make sure the pair does not ever form a link
				// if (!exclusionSet.contains(charOneId + "_" + charTwoId)) {
				if (!(exclusionSet.contains(t1 + "_" + t2) || exclusionSet
						.contains(t2 + "_" + t1))) {

					// we have a negative sample!!
					insertEdgeSample(conn, pairId++, t1, t2, NEGATIVE_LABEL);
					COUNT_EDGE_SAMPLE++;
					// System.out.println("num = " + COUNT_EDGE_SAMPLE);
				}
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

		return COUNT_EDGE_SAMPLE;
	}

	private void deleteExistingEdgeSamples(String formLink) {

		COUNT_EDGE_SAMPLE = 0;
		Connection conn = ConnectionUtil.getGUILEConnection(false);
		try {
			Statement stmt = conn.createStatement();
			stmt.executeUpdate("delete from bbn_training_period_pairs "
					+ "WHERE form_link = '" + formLink + "'");
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

	public void deleteAllExistingEdgeSamples() {

		COUNT_EDGE_SAMPLE = 0;
		Connection conn = ConnectionUtil.getGUILEConnection(false);
		try {
			Statement stmt = conn.createStatement();
			stmt.executeUpdate("delete from bbn_training_period_pairs");
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

	protected Set<Integer> getTrainingPlayerAccounts(Connection conn)
			throws SQLException {

		Statement stmt = conn.createStatement();
		// load all distinct individual training players into memory
		ResultSet rs = stmt
				.executeQuery("select distinct(char_id) from bbn_training_period_player");
		Set<Integer> trainingPlayersSet = new HashSet<Integer>();
		while (rs.next()) {
			trainingPlayersSet.add(rs.getInt("char_id"));
		}

		return trainingPlayersSet;
	}

	protected void insertEdgeSample(Connection conn, int id, int charOneId,
			int charTwoId, String formLink) throws SQLException {

		PreparedStatement ps = conn
				.prepareStatement("INSERT INTO bbn_training_period_pairs"
						+ "(player_pairs_id, player1_char_id, "
						+ "player2_char_id, form_link) VALUES (?, ?, ?, ?)");

		ps.setInt(1, id);
		ps.setInt(2, charOneId);
		ps.setInt(3, charTwoId);
		ps.setString(4, formLink);
		ps.executeUpdate();
		// System.out.println("Inserted " + COUNT_EDGE_SAMPLE + " - " +
		// formLink);
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
