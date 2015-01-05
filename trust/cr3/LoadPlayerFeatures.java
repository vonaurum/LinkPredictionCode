package linkpred.trust.cr3;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import linkpred.trust.bean.TrustNode;
import util.ConnectionUtil;
import etl.bean.Month;

public class LoadPlayerFeatures {

	private static int BATCH_SIZE = 500;

	private static final String MAX_ID_QUERY = "select MAX(lp_cr3_tp_features_id) AS max_num from lp_cr3_tp_features";

	private static int MAX_ID = 0;

	private static final String INSERT_NEW_CR3_TP_FEATURE = "INSERT INTO lp_cr3_tp_features (lp_cr3_tp_features_id, "
			+ "account, char_id, real_gender, location, age_2011, month_range) VALUES (?,?,?,?,?,?,?)";

	private Map<Long, TrustNode> GLOBAL_PLAYER_MAP = new HashMap<Long, TrustNode>();

	public void loadPlayerStaticFeatures(int serverId, String monthRange) {

		Connection conn_src = null;
		Connection conn_dest = null;

		try {

			conn_src = ConnectionUtil.getNCSAConnection();
			conn_dest = getDestConn();
			conn_dest.setAutoCommit(false);
			PreparedStatement stmt_dest = conn_dest
					.prepareStatement(INSERT_NEW_CR3_TP_FEATURE);

			MAX_ID = (int) getMaxId(conn_dest);
			System.out.println("MAX_ID = " + MAX_ID);

			Statement stmt = conn_src.createStatement();
			System.out.println("*** Start: fetch data");
			String SQL_FETCH_ALL_STATIC_INFO = "select r.roleindexid, r.rolename, s.location, s.birth_year, "
					+ "s.birth_month, s.gender from KS_RoleIndex r LEFT OUTER JOIN KS_SurveyData_Clean s "
					+ "ON r.AccountName = s.acct_usrn";
			System.out.println("Query: " + SQL_FETCH_ALL_STATIC_INFO);
			ResultSet rs = stmt.executeQuery(SQL_FETCH_ALL_STATIC_INFO);
			System.out.println("*** End: fetch data");
			if (rs != null) {
				System.out.println("*** Start: insert data");
				System.out.println("Insert stmt: " + INSERT_NEW_CR3_TP_FEATURE);
				long count = 0;
				while (rs.next()) {
					try {
						TrustNode trustNode = new TrustNode();
						trustNode.setAccount(rs.getString("rolename"));
						trustNode.setCharacterId(rs.getLong("roleindexid"));
						trustNode.setMonthRange(monthRange);

						trustNode.setCountry(rs.getString("location"));
						trustNode.setRealGender(rs.getString("gender"));
						Integer y = rs.getInt("birth_year");
						if (y != null && y > 0) {
							trustNode.setAge2011(2011 - y);
						}

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
		nullSafeSet(ps, 2, trustNode.getAccount(), String.class);
		ps.setLong(3, trustNode.getCharacterId());
		nullSafeSet(ps, 4, trustNode.getRealGender(), String.class);
		nullSafeSet(ps, 5, trustNode.getCountry(), String.class);
		nullSafeSet(ps, 6, trustNode.getAge2011(), Integer.class);
		nullSafeSet(ps, 7, trustNode.getMonthRange(), String.class);
		ps.addBatch();
	}

	private long getMaxId(Connection conn) throws SQLException {

		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(MAX_ID_QUERY);

		return (rs != null && rs.next()) ? rs.getLong("max_num") : 0;
	}

	public void loadPlayerDynamicIndividualFeatures(Month startMonth,
			Month endMonth, int serverId) {

		System.out.println("*** Loading characters into memory..");
		loadCharacters(startMonth, endMonth);
		System.out.println("*** Calculating max char level for characters..");
		calculateLevelStats(startMonth, endMonth);
		System.out
				.println("*** Calculating total session length for characters..");
		calculateDistributionStats(startMonth, endMonth);
		Connection conn_dest = getDestConn();
		try {

			String updateStmt = "update lp_cr3_tp_features set max_char_level = ?, total_sl_mins = ? where month_range = ? and char_id = ?";
			System.out.println("Update statement: " + updateStmt);
			PreparedStatement stmt_update = conn_dest
					.prepareStatement(updateStmt);
			long count = 0;
			for (TrustNode player : GLOBAL_PLAYER_MAP.values()) {
				if (player.getMaxCharLevel() != null
						|| player.getTotalSessionLengthMins() != null) {
					// System.out.println(player);
					addToBatch(stmt_update, player.getMaxCharLevel(),
							player.getTotalSessionLengthMins(),
							Month.getMonthsRange(startMonth, endMonth),
							player.getCharacterId());
				}
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
			Integer totalSLMins, String monthRange, long charId)
			throws SQLException {
		nullSafeSet(ps, 1, maxCharLevel, Integer.class);
		nullSafeSet(ps, 2, totalSLMins, Integer.class);
		ps.setString(3, monthRange);
		ps.setLong(4, charId);
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

	private void loadCharacters(Month startMonth, Month endMonth) {

		GLOBAL_PLAYER_MAP = new HashMap<Long, TrustNode>();
		Connection conn = getSourceConn();
		try {
			Statement stmt = conn.createStatement();
			System.out.println("** Start: fetch data");
			String query = "select distinct char_id from lp_cr3_tp_features where month_range = '"
					+ Month.getMonthsRange(startMonth, endMonth) + "'";
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

	private void calculateLevelStats(Month startMonth, Month endMonth) {

		Connection conn = ConnectionUtil.getHedgeHogConnection();
		try {
			Statement stmt = conn.createStatement();
			System.out.println("** Start: fetch data");
			String QUERY = "select ROLEINDEXID, max(actiontarget) AS max_level from cr3_action_level "
					+ "where MONTH(ACTIONTIME) BETWEEN "
					+ startMonth.getNumber()
					+ " AND "
					+ endMonth.getNumber()
					+ " group by ROLEINDEXID";
			System.out.println("Query: " + QUERY);
			ResultSet rs = stmt.executeQuery(QUERY);
			System.out.println("** End: fetch data");
			if (rs != null) {
				while (rs.next()) {
					Long charId = rs.getLong("ROLEINDEXID");
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

	private void calculateDistributionStats(Month startMonth, Month endMonth) {

		Connection conn = getSourceConn();
		try {
			Statement stmt = conn.createStatement();
			System.out.println("** Start: fetch data");
			String QUERY = "select game_character_id, SUM(session_length_secs)/60 AS sl_mins "
					+ "from cr3_char_level_session where MONTH IN ("
					+ Month.getMonthsQueryCSV(startMonth, endMonth)
					+ ") group by game_character_id";
			System.out.println("Query: " + QUERY);
			ResultSet rs = stmt.executeQuery(QUERY);
			System.out.println("** End: fetch data");
			if (rs != null) {
				while (rs.next()) {
					try {
						Long characterId = rs.getLong("game_character_id");
						TrustNode currPlayer = GLOBAL_PLAYER_MAP
								.get(characterId);
						if (currPlayer != null) {
							currPlayer.setTotalSessionLengthMins(rs
									.getInt("sl_mins"));
						}
					} catch (SQLException e) {
						e.printStackTrace();
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

	private Connection getSourceConn() {
		return ConnectionUtil.getGUILETrustConnection();
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
