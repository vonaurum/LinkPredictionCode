package linkpred.trust.eq2;

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
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import linkpred.trust.bean.TrustNode;
import util.ConnectionUtil;
import etl.bean.Month;

public class LoadPlayerFeatures {

	private static int BATCH_SIZE = 500;

	private static final String MAX_ID_QUERY = "select MAX(lp_player_tp_features_id) AS max_num from lp_player_tp_features";

	private static int MAX_ID = 0;

	private static final String INSERT_NEW_PLAYER_TP_FEATURE = "INSERT INTO lp_player_tp_features (lp_player_tp_features_id, server_id, account, char_id, "
			+ "real_gender, country, age_2006, age_joining, char_class_id, cs_char_level, char_gender, char_race, guild_id, guild_rank, month_range) "
			+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

	private Map<Long, TrustNode> GLOBAL_PLAYER_MAP = new HashMap<Long, TrustNode>();

	public void loadPlayerStaticFeatures(int serverId, String monthRange) {

		Connection conn_src = null;
		Connection conn_dest = null;

		try {

			conn_src = getSourceConn();
			conn_dest = getDestConn();
			conn_dest.setAutoCommit(false);
			PreparedStatement stmt_dest = conn_dest
					.prepareStatement(INSERT_NEW_PLAYER_TP_FEATURE);

			MAX_ID = (int) getMaxId(conn_dest);
			System.out.println("MAX_ID = " + MAX_ID);

			Statement stmt = conn_src.createStatement();
			System.out.println("*** Start: fetch data");
			String SQL_FETCH_ALL_STATIC_INFO = "select c.account, c.character_id, c.race, c.gender AS char_gender, c.class_id, c.char_level, "
					+ "c.guild_id, c.guild_rank, d.birth_date, d.creation_date, d.gender AS real_gender, d.country_code "
					+ "from eq2_char_store_min c, eq2_demographics d where c.account = d.account and c.server_id =  "
					+ serverId;
			System.out.println("Query: " + SQL_FETCH_ALL_STATIC_INFO);
			ResultSet rs = stmt.executeQuery(SQL_FETCH_ALL_STATIC_INFO);
			System.out.println("*** End: fetch data");
			if (rs != null) {
				System.out.println("*** Start: insert data");
				System.out.println("Insert stmt: "
						+ INSERT_NEW_PLAYER_TP_FEATURE);
				long count = 0;
				while (rs.next()) {
					try {
						TrustNode trustNode = new TrustNode();
						trustNode.setServerId(serverId);
						trustNode.setAccountId(rs.getLong("account"));
						trustNode.setCharacterId(rs.getLong("character_id"));
						trustNode.setCharRace(rs.getInt("race"));
						trustNode.setCharGender(rs.getInt("char_gender"));
						trustNode.setCharClassId(rs.getInt("class_id"));
						trustNode.setCsCharLevel(rs.getInt("char_level"));
						trustNode.setGuildId(nullSafeGet(rs
								.getObject("guild_id")));
						trustNode.setGuildRank(nullSafeGet(rs
								.getObject("guild_rank")));
						trustNode.setMonthRange(monthRange);

						trustNode.setCountry(rs.getString("country_code"));
						trustNode.setRealGender(rs.getString("real_gender"));
						trustNode.setAge2006(calculateAgeAt2006(rs
								.getString("birth_date")));
						trustNode.setAgeAtJoining(calculateAgeAtjoining(
								rs.getString("creation_date"),
								rs.getString("birth_date")));

						addNodeToBatch(stmt_dest, trustNode);
						count++;
						if (count % BATCH_SIZE == 0) {
							stmt_dest.executeBatch();
							conn_dest.commit();
						}
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
				stmt_dest.executeBatch();
				conn_dest.commit();
				System.out.println("*** End: insert data");
			}
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

		System.out.println("Loaded basic static data");
	}

	private void addNodeToBatch(PreparedStatement ps, TrustNode trustNode)
			throws SQLException {

		ps.setInt(1, ++MAX_ID);
		ps.setInt(2, trustNode.getServerId());
		nullSafeSet(ps, 3, trustNode.getAccountId(), Long.class);
		ps.setLong(4, trustNode.getCharacterId());
		nullSafeSet(ps, 5, trustNode.getRealGender(), String.class);
		nullSafeSet(ps, 6, trustNode.getCountry(), String.class);
		nullSafeSet(ps, 7, trustNode.getAge2006(), Integer.class);
		nullSafeSet(ps, 8, trustNode.getAgeAtJoining(), Integer.class);
		nullSafeSet(ps, 9, trustNode.getCharClassId(), Integer.class);
		nullSafeSet(ps, 10, trustNode.getCsCharLevel(), Integer.class);
		nullSafeSet(ps, 11, trustNode.getCharGender(), Integer.class);
		nullSafeSet(ps, 12, trustNode.getCharRace(), Integer.class);
		nullSafeSet(ps, 13, trustNode.getGuildId(), Integer.class);
		nullSafeSet(ps, 14, trustNode.getGuildRank(), Integer.class);
		nullSafeSet(ps, 15, trustNode.getMonthRange(), String.class);
		ps.addBatch();
	}

	private long getMaxId(Connection conn) throws SQLException {

		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(MAX_ID_QUERY);

		return (rs != null && rs.next()) ? rs.getLong("max_num") : 0;
	}

	private static int calculateAgeAt2006(String strDate) {

		int ret = -99;
		if (strDate != null && !strDate.trim().equals("")) {
			DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
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
			DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
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

	public static void main(String args[]) {

		System.out.println((int) (double) Double.valueOf("11.1"));
	}

	public void loadPlayerDynamicIndividualFeatures(Month startMonth,
			Month endMonth, int serverId) {

		System.out.println("*** Loading characters into memory..");
		loadCharacters(startMonth, endMonth, serverId);
		System.out.println("*** Calculating max char level for characters..");
		calculateLevelStats(startMonth, endMonth, getServerName(serverId));
		System.out
				.println("*** Calculating total session length for characters..");
		calculateDistributionStats(startMonth, endMonth,
				getServerName(serverId));
		Connection conn_dest = getDestConn();
		try {

			String updateStmt = "update lp_player_tp_features set max_char_level = ?, total_sl_mins = ? where server_id = ? and month_range = ? and char_id = ?";
			System.out.println("Update statement: " + updateStmt);
			PreparedStatement stmt_update = conn_dest
					.prepareStatement(updateStmt);
			long count = 0;
			for (TrustNode player : GLOBAL_PLAYER_MAP.values()) {
				// System.out.println(player);
				addToBatch(stmt_update, player.getMaxCharLevel(),
						player.getTotalSessionLengthMins(), serverId,
						Month.getMonthsRange(startMonth, endMonth),
						player.getCharacterId());
				count++;
				if (count % BATCH_SIZE == 0) {
					System.out.println("Batch committed. count = " + count);
					stmt_update.executeBatch();
					conn_dest.commit();
				}
			}
			stmt_update.executeBatch();
			conn_dest.commit();
		} catch (SQLException e) {
			System.out.println();
			e.printStackTrace();
		} finally {
			try {
				conn_dest.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		System.out.println("*** Completed!!");
	}

	private void addToBatch(PreparedStatement ps, Integer maxCharLevel,
			Integer totalSLMins, int serverId, String monthRange, long charId)
			throws SQLException {
		nullSafeSet(ps, 1, maxCharLevel, Integer.class);
		nullSafeSet(ps, 2, totalSLMins, Integer.class);
		ps.setInt(3, serverId);
		ps.setString(4, monthRange);
		ps.setLong(5, charId);
		ps.addBatch();
		// ps.executeUpdate();
	}

	private String getServerName(int serverId) {
		switch (serverId) {
		case 101:
			return "guk";
		case 120:
			return "nagafen";
		}
		return null;
	}

	private void loadCharacters(Month startMonth, Month endMonth, int serverId) {

		GLOBAL_PLAYER_MAP = new HashMap<Long, TrustNode>();
		Connection conn = getSourceConn();
		try {
			Statement stmt = conn.createStatement();
			System.out.println("** Start: fetch data");
			String query = "select distinct char_id from lp_player_tp_features where month_range = '"
					+ Month.getMonthsRange(startMonth, endMonth)
					+ "' and server_id = " + serverId;
			System.out.println("Query: " + query);
			ResultSet rs = stmt.executeQuery(query);
			System.out.println("** End: fetch data");
			while (rs.next()) {
				Long charId = rs.getLong("char_id");
				GLOBAL_PLAYER_MAP.put(charId, new TrustNode(charId));
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

	private void calculateLevelStats(Month startMonth, Month endMonth,
			String serverName) {

		Connection conn = getSourceConn();
		try {
			Statement stmt = conn.createStatement();
			System.out.println("** Start: fetch data");
			String QUERY = "select account_id, game_character_id, MAX(game_character_level) AS max_level "
					+ "from account_char_level_month where MONTH IN ("
					+ Month.getMonthsQueryCSV(startMonth, endMonth)
					+ ") and server_name = '"
					+ serverName
					+ "' group by account_id, game_character_id order by account_id, game_character_id";
			System.out.println("Query: " + QUERY);
			ResultSet rs = stmt.executeQuery(QUERY);
			System.out.println("** End: fetch data");
			if (rs != null) {
				while (rs.next()) {
					Long charId = rs.getLong("game_character_id");
					TrustNode player = GLOBAL_PLAYER_MAP.get(charId);
					if (player != null) {
						player.setMaxCharLevel(rs.getInt("max_level"));
						// System.out.println("found");
					} else {
						System.out.println("Char not found: " + charId);
					}
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

	private void calculateDistributionStats(Month startMonth, Month endMonth,
			String serverName) {

		Connection conn = getSourceConn();
		try {
			Statement stmt = conn.createStatement();
			System.out.println("** Start: fetch data");
			String QUERY = "select account_id, game_character_id, game_character_level, sl_dist_mins "
					+ "from account_char_level_month where MONTH IN ("
					+ Month.getMonthsQueryCSV(startMonth, endMonth)
					+ ") and server_name = '"
					+ serverName
					+ "' order by account_id, game_character_id,game_character_level";
			System.out.println("Query: " + QUERY);
			ResultSet rs = stmt.executeQuery(QUERY);
			System.out.println("** End: fetch data");
			if (rs != null) {
				TrustNode currPlayer = null;
				int numLevels = 0;
				Integer totalSessionLengthMins = 0;
				while (rs.next()) {
					try {
						Long characterId = rs.getLong("game_character_id");
						String sessionLengthDist = rs.getString("sl_dist_mins");
						// first account_character
						if (currPlayer == null) {
							// System.out.println("First session");
							currPlayer = GLOBAL_PLAYER_MAP.get(characterId);
						} else {
							// first entry for an account-char
							if (!(characterId.equals(currPlayer
									.getCharacterId()))) {
								// compute metrics of distributions
								currPlayer
										.setTotalSessionLengthMins(totalSessionLengthMins);

								currPlayer = GLOBAL_PLAYER_MAP.get(characterId);
								numLevels = 0;
								totalSessionLengthMins = 0;
							}
						}
						totalSessionLengthMins += getSumValues(sessionLengthDist);
						numLevels++;
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}

				// for the last distribution
				if (currPlayer != null) {
					currPlayer
							.setTotalSessionLengthMins(totalSessionLengthMins);
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

	private static Integer getSumValues(String csvValues) {
		Integer ret = null;
		if (csvValues != null && csvValues.length() > 0) {
			ret = 0;
			String[] values = csvValues.split(",");
			for (String value : values) {
				ret += (int) (double) Double.valueOf(value);
			}
		}
		return ret;
	}

	private Connection getSourceConn() {
		return ConnectionUtil.getGUILETrustConnection(false);
	}

	private Connection getDestConn() {
		return ConnectionUtil.getGUILETrustConnection(false);
	}

	private Integer nullSafeGet(Object obj) throws SQLException {

		Integer ret = null;
		if (obj != null && !obj.equals("")) {
			ret = Integer.valueOf((String) obj);
		}

		return ret;
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
