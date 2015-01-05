/*
 * BBNFeatureId.java
 *
 * Created on Jul 12, 2010 
 */
package linkpred.superlearn.bbn;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Zoheb Borbora
 * 
 */
public enum BBNFeatureId {

	/**
	 * Feature set:
	 * 
	 * <pre>
	 * 
	 * 	[player_pairs_id] [int] NOT NULL,
	 * 	[player1_char_id] [int] NULL,
	 * 	[player2_char_id] [int] NULL,
	 * 	0 - [real_gender_indicator] [int] NULL,
	 * 	1 - [real_country_indicator] [int] NULL,
	 * 	2 - [game_class_indicator] [int] NULL,
	 * 	3 - [game_gender_indicator] [int] NULL,
	 * 	4 - [game_race_indicator] [int] NULL,
	 * 	5 - [sum_age] [int] NULL,
	 * 	6 - [diff_age] [int] NULL,
	 * 	7 - [sum_joining_age] [int] NULL,
	 * 	8 - [diff_joining_age] [int] NULL,
	 * 	9 - [sum_char_level] [int] NULL,
	 * 	10 - [diff_char_level] [int] NULL,
	 * 	11 - [sum_neighbors] [int] NULL,
	 * 	12 - [diff_degree] [real] NULL,
	 * 	13 - [sum_actions] [int] NULL,
	 * 	14 - [shortest_ditsance] [int] NULL,
	 * 	15 - [sum_clustering_index] [float] NULL,
	 * 	16 - [common_neighbors] [int] NULL,
	 * 	17 - [aa_index] [float] NULL,
	 * 	18 - [ra_index] [float] NULL,
	 * 	19 - [diff_degree_cent] [real] NULL,
	 * 	20 - [diff_betweenness_cent] [real] NULL,
	 * 	21 - [diff_closeness_cent] [real] NULL,
	 * 	22 - [diff_eigenvector_cent] [real] NULL,
	 * 	[form_link] [varchar](2) NULL,
	 * 	23 - [wcn_index] [float] NULL,
	 * 	24 - [waa_index] [float] NULL,
	 * 	25 - [wra_index] [float] NULL,
	 * 	26 - [wcn_index2] [float] NULL,
	 * 	27 - [waa_index2] [float] NULL,
	 * 	28 - [wra_index2] [float] NULL,
	 * 	29 - [wcn_index3] [float] NULL,
	 * 	30 - [waa_index3] [float] NULL,
	 * 	31 - [wra_index3] [float] NULL,
	 * 	32 - [wcn_index4] [float] NULL,
	 * 	33 - [waa_index4] [float] NULL,
	 * 	34 - [wra_index4] [float] NULL,
	 * 	35 - [wcn_index_1] [float] NULL,
	 * 	36 - [waa_index_1] [float] NULL,
	 * 	37 - [wra_index_1] [float] NULL,
	 * 	38 - [wcn_index_2] [float] NULL,
	 * 	39 - [waa_index_2] [float] NULL,
	 * 	40 - [wra_index_2] [float] NULL,
	 * 	41 - [wcn_index_3] [float] NULL,
	 * 	42 - [waa_index_3] [float] NULL,
	 * 	43 - [wra_index_3] [float] NULL,
	 * 	44 - [wcn_index_4] [float] NULL,
	 * 	45 - [waa_index_4] [float] NULL,
	 * 	46 - [wra_index_4] [float] NULL,
	 * 
	 * </pre>
	 */

	ZERO(0, "real_gender_indicator"), ONE(1, "real_country_indicator"), TWO(2,
			"game_class_indicator"), THREE(3, "game_gender_indicator"), FOUR(4,
			"game_race_indicator"), FIVE(5, "sum_age"), SIX(6, "diff_age"), SEVEN(
			7, "sum_joining_age"), EIGHT(8, "diff_joining_age"), NINE(9,
			"sum_char_level"), TEN(10, "diff_char_level"), ELEVEN(11,
			"sum_neighbors"), TWELVE(12, "diff_degree"), THIRTEEN(13,
			"sum_actions"), FOURTEEN(14, "shortest_ditsance"), FIFTEEN(15,
			"sum_clustering_index"), SIXTEEN(16, "common_neighbors"), SEVENTEEN(
			17, "aa_index"), EIGHTEEN(18, "ra_index"), NINETEEN(19,
			"diff_degree_cent"), TWENTY(20, "diff_betweenness_cent"), TWENTY_ONE(
			21, "diff_closeness_cent"), TWENTY_TWO(22, "diff_eigenvector_cent"), TWENTY_THREE(
			23, "wcn_index"), TWENTY_FOUR(24, "waa_index"), TWENTY_FIVE(25,
			"wra_index"), TWENTY_SIX(26, "wcn_index2"), TWENTY_SEVEN(27,
			"waa_index2"), TWENTY_EIGHT(28, "wra_index2"), TWENTY_NINE(29,
			"wcn_index3"), THIRTY(30, "waa_index3"), THIRTY_ONE(31,
			"wra_index3"), THIRTY_TWO(32, "wcn_index4"), THIRTY_THREE(33,
			"waa_index4"), THIRTY_FOUR(34, "wra_index4"), THIRTY_FIVE(35,
			"wcn_index_1"), THIRTY_SIX(36, "waa_index_1"), THIRTY_SEVEN(37,
			"wra_index_1"), THIRTY_EIGHT(38, "wcn_index_2"), THIRTY_NINE(39,
			"waa_index_2"), FORTY(40, "wra_index_2"), FORTY_ONE(41,
			"wcn_index_3"), FORTY_TWO(42, "waa_index_3"), FORTY_THREE(43,
			"wra_index_3"), FORTY_FOUR(44, "wcn_index_4"), FORTY_FIVE(45,
			"waa_index_4"), FORTY_SIX(46, "wra_index_4");

	private static Map<Integer, BBNFeatureId> ID_TYPE_MAP = new HashMap<Integer, BBNFeatureId>();
	static {
		ID_TYPE_MAP.put(0, ZERO);
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
	}

	private int id;

	private String name;

	private BBNFeatureId(int id, String name) {
		this.id = id;
		this.name = name;
	}

	public int getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public static BBNFeatureId getTypeById(int id) {
		return ID_TYPE_MAP.get(id);
	}

	public static void main(String[] args) {

		for (int i = 0; i <= 40; i++) {
			System.out.println(i + " " + getTypeById(i).getName());
		}
	}
}
