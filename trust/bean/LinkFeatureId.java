/*
 * LinkFeatureId.java
 *
 * Created on Jul 12, 2010 
 */
package linkpred.trust.bean;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Zoheb Borbora
 * 
 */
public enum LinkFeatureId {

	/**
	 * Feature set:
	 * 
	 * <pre>
	 * 
	 * 	[player_pairs_id] [int] NOT NULL,
	 * 	[player1_char_id] [int] NULL,
	 * 	[player2_char_id] [int] NULL,
	 * 1 - real_gender_indicator
	 * 2 - game_gender_indicator 
	 * 3 - game_race_indicator 
	 * 4 - real_country_indicator 
	 * 5 - sum_age 
	 * 6 - sum_char_sl_mins
	 * 7 - diff_age 
	 * 8 - diff_char_sl_mins
	 * 9 - sum_joining_age 
	 * 10 - diff_joining_age 
	 * 11 - game_class_indicator 
	 * 12 - sum_char_level
	 * 13 - diff_char_level
	 * 14 - guild_indicator
	 * 15 - sum_guild_rank
	 * 16 - diff_guild_rank
	 * 17 - diff_degree_cent
	 * 18 - diff_betweenness_cent
	 * 19 - diff_closeness_cent
	 * 20 - diff_eigenvector_cent
	 * 21 - sum_degree
	 * 22 - diff_degree
	 * 23 - shortest_distance
	 * 24 - sum_clustering_index
	 * 25 - common_neighbors
	 * 26 - salton_index
	 * 27 - jaccard_index
	 * 28 - sorensen_index
	 * 29 - aa_index
	 * 30 - ra_index
	 * 31 - link_in_housing
	 * 32 - link_in_mentoring
	 * 33 - link_in_trade
	 * 34 - link_in_group
	 * 35 - link_in_pvp
	 * 36 - wcn_index 
	 * 37 - wcn_index2 
	 * 38 - wcn_index3 
	 * 39 - wcn_index4
	 * 40 - wcn_index_1 
	 * 41 - wcn_index_2
	 * 42 - wcn_index_3
	 * 43 - wcn_index_4
	 * 44 - waa_index
	 * 45 - waa_index2
	 * 46 - waa_index3
	 * 47 - waa_index4
	 * 48 - waa_index_1
	 * 49 - waa_index_2
	 * 50 - waa_index_3
	 * 51 - waa_index_4
	 * 52 - wra_index 
	 * 53 - wra_index2 
	 * 54 - wra_index3 
	 * 55 - wra_index4
	 * 56 - wra_index_1 
	 * 57 - wra_index_2 
	 * 58 - wra_index_3
	 * 59 - wra_index_4
	 * 60 - link_in_pos
	 * 61 - link_in_neg
	 * 62 - link_in_friend
	 * 63 - link_in_team
	 * </pre>
	 */

	ONE(1, "real_gender_indicator"), TWO(2, "game_gender_indicator"), THREE(3,
			"game_race_indicator"), FOUR(4, "real_country_indicator"), FIVE(5,
			"sum_age"), SIX(6, "sum_char_sl_mins"), SEVEN(7, "diff_age"), EIGHT(
			8, "diff_char_sl_mins"), NINE(9, "sum_joining_age"), TEN(10,
			"diff_joining_age"), ELEVEN(11, "game_class_indicator"), TWELVE(12,
			"sum_char_level"), THIRTEEN(13, "diff_char_level"), FOURTEEN(14,
			"guild_indicator"), FIFTEEN(15, "sum_guild_rank"), SIXTEEN(16,
			"diff_guild_rank"), SEVENTEEN(17, "diff_degree_cent"), EIGHTEEN(18,
			"diff_betweenness_cent"), NINETEEN(19, "diff_closeness_cent"), TWENTY(
			20, "diff_eigenvector_cent"), TWENTY_ONE(21, "sum_degree"), TWENTY_TWO(
			22, "diff_degree"), TWENTY_THREE(23, "shortest_distance"), TWENTY_FOUR(
			24, "sum_clustering_index"), TWENTY_FIVE(25, "common_neighbors"), TWENTY_SIX(
			26, "salton_index"), TWENTY_SEVEN(27, "jaccard_index"), TWENTY_EIGHT(
			28, "sorensen_index"), TWENTY_NINE(29, "aa_index"), THIRTY(30,
			"ra_index"), THIRTY_ONE(31, "link_in_housing"), THIRTY_TWO(32,
			"link_in_mentoring"), THIRTY_THREE(33, "link_in_trade"), THIRTY_FOUR(
			34, "link_in_group"), THIRTY_FIVE(35, "link_in_pvp"), THIRTY_SIX(
			36, "wcn_index"), THIRTY_SEVEN(37, "wcn_index2"), THIRTY_EIGHT(38,
			"wcn_index3"), THIRTY_NINE(39, "wcn_index4"), FORTY(40,
			"wcn_index_1"), FORTY_ONE(41, "wcn_index_2"), FORTY_TWO(42,
			"wcn_index_3"), FORTY_THREE(43, "wcn_index_4"), FORTY_FOUR(44,
			"waa_index"), FORTY_FIVE(45, "waa_index2"), FORTY_SIX(46,
			"waa_index3"), FORTY_SEVEN(47, "waa_index4"), FORTY_EIGHT(48,
			"waa_index_1"), FORTY_NINE(49, "waa_index_2"), FIFTY(50,
			"waa_index_3"), FIFTY_ONE(51, "waa_index_4"), FIFTY_TWO(52,
			"wra_index"), FIFTY_THREE(53, "wra_index2"), FIFTY_FOUR(54,
			"wra_index3"), FIFTY_FIVE(55, "wra_index4"), FIFTY_SIX(56,
			"wra_index_1"), FIFTY_SEVEN(57, "wra_index_2"), FIFTY_EIGHT(58,
			"wra_index_3"), FIFTY_NINE(59, "wra_index_4"), SIXTY(60,
			"link_in_pos"), SIXTY_ONE(61, "link_in_neg"), SIXTY_TWO(62,
			"link_in_friend"), SIXTY_THREE(63, "link_in_team");

	private static Map<Integer, LinkFeatureId> ID_TYPE_MAP = new HashMap<Integer, LinkFeatureId>();
	static {
		ID_TYPE_MAP.put(1, ONE);
		ID_TYPE_MAP.put(2, TWO);
		ID_TYPE_MAP.put(3, THREE);
		ID_TYPE_MAP.put(4, FOUR);
		ID_TYPE_MAP.put(5, FIVE);
		ID_TYPE_MAP.put(6, SIX);
		ID_TYPE_MAP.put(7, SEVEN);
		ID_TYPE_MAP.put(8, EIGHT);
		ID_TYPE_MAP.put(9, NINE);
		ID_TYPE_MAP.put(10, TEN);
		ID_TYPE_MAP.put(11, ELEVEN);
		ID_TYPE_MAP.put(12, TWELVE);
		ID_TYPE_MAP.put(13, THIRTEEN);
		ID_TYPE_MAP.put(14, FOURTEEN);
		ID_TYPE_MAP.put(15, FIFTEEN);
		ID_TYPE_MAP.put(16, SIXTEEN);
		ID_TYPE_MAP.put(17, SEVENTEEN);
		ID_TYPE_MAP.put(18, EIGHTEEN);
		ID_TYPE_MAP.put(19, NINETEEN);
		ID_TYPE_MAP.put(20, TWENTY);
		ID_TYPE_MAP.put(21, TWENTY_ONE);
		ID_TYPE_MAP.put(22, TWENTY_TWO);
		ID_TYPE_MAP.put(23, TWENTY_THREE);
		ID_TYPE_MAP.put(24, TWENTY_FOUR);
		ID_TYPE_MAP.put(25, TWENTY_FIVE);
		ID_TYPE_MAP.put(26, TWENTY_SIX);
		ID_TYPE_MAP.put(27, TWENTY_SEVEN);
		ID_TYPE_MAP.put(28, TWENTY_EIGHT);
		ID_TYPE_MAP.put(29, TWENTY_NINE);
		ID_TYPE_MAP.put(30, THIRTY);
		ID_TYPE_MAP.put(31, THIRTY_ONE);
		ID_TYPE_MAP.put(32, THIRTY_TWO);
		ID_TYPE_MAP.put(33, THIRTY_THREE);
		ID_TYPE_MAP.put(34, THIRTY_FOUR);
		ID_TYPE_MAP.put(35, THIRTY_FIVE);
		ID_TYPE_MAP.put(36, THIRTY_SIX);
		ID_TYPE_MAP.put(37, THIRTY_SEVEN);
		ID_TYPE_MAP.put(38, THIRTY_EIGHT);
		ID_TYPE_MAP.put(39, THIRTY_NINE);
		ID_TYPE_MAP.put(40, FORTY);
		ID_TYPE_MAP.put(41, FORTY_ONE);
		ID_TYPE_MAP.put(42, FORTY_TWO);
		ID_TYPE_MAP.put(43, FORTY_THREE);
		ID_TYPE_MAP.put(44, FORTY_FOUR);
		ID_TYPE_MAP.put(45, FORTY_FIVE);
		ID_TYPE_MAP.put(46, FORTY_SIX);
		ID_TYPE_MAP.put(47, FORTY_SEVEN);
		ID_TYPE_MAP.put(48, FORTY_EIGHT);
		ID_TYPE_MAP.put(49, FORTY_NINE);
		ID_TYPE_MAP.put(50, FIFTY);
		ID_TYPE_MAP.put(51, FIFTY_ONE);
		ID_TYPE_MAP.put(52, FIFTY_TWO);
		ID_TYPE_MAP.put(53, FIFTY_THREE);
		ID_TYPE_MAP.put(54, FIFTY_FOUR);
		ID_TYPE_MAP.put(55, FIFTY_FIVE);
		ID_TYPE_MAP.put(56, FIFTY_SIX);
		ID_TYPE_MAP.put(57, FIFTY_SEVEN);
		ID_TYPE_MAP.put(58, FIFTY_EIGHT);
		ID_TYPE_MAP.put(59, FIFTY_NINE);
		ID_TYPE_MAP.put(60, SIXTY);
		ID_TYPE_MAP.put(61, SIXTY_ONE);
		ID_TYPE_MAP.put(62, SIXTY_TWO);
		ID_TYPE_MAP.put(63, SIXTY_THREE);
	}

	private int id;

	private String name;

	private LinkFeatureId(int id, String name) {
		this.id = id;
		this.name = name;
	}

	public int getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public static LinkFeatureId getTypeById(int id) {
		return ID_TYPE_MAP.get(id);
	}

	public static void main(String[] args) {

		for (int i = 0; i <= 40; i++) {
			System.out.println(i + " " + getTypeById(i).getName());
		}
	}
}
