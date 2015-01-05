package linkpred.trust.bean;

public enum LPNetwork {

	GUK_HOUSING("HOUSING", "guk_housing_net_feb_jun",
			"guk_housing_net_jul_aug", "src_char_id", "dest_char_id", "trust",
			"lp_trust_edge"), GUK_MENTORING("MENTORING",
			"guk_mentoring_net_feb_jun", "guk_mentoring_net_jul_aug",
			"mentor_character_id", "apprentice_character_id", "mentoring",
			"lp_mentoring_edge"), GUK_TRADE("TRADE",
			"guk_trade_network_feb_jun", "guk_trade_network_jul_aug",
			"src_char_id", "dest_char_id", "trade", "lp_trade_edge"), GUK_GROUP(
			"GROUP", "guk_group_graph_feb_jun", "guk_group_graph_jul_aug",
			"src_char_id", "dest_char_id", "group", "lp_group_edge"), NAGAFEN_HOUSING(
			"HOUSING", "nagafen_housing_net_feb_jun",
			"nagafen_housing_net_jul_aug", "src_char_id", "dest_char_id",
			"trust", "lp_trust_edge"), NAGAFEN_MENTORING("MENTORING",
			"nagafen_mentoring_net_feb_jun", "nagafen_mentoring_net_jul_aug",
			"mentor_character_id", "apprentice_character_id", "mentoring",
			"lp_mentoring_edge"), NAGAFEN_TRADE("TRADE",
			"nagafen_trade_network_feb_jun", "nagafen_trade_network_jul_aug",
			"src_char_id", "dest_char_id", "trade", "lp_trade_edge"), NAGAFEN_PVP(
			"PVP", "nagafen_pvp_feb_jun", "nagafen_pvp_jul_aug", "src_char_id",
			"dest_char_id", "pvp", "lp_pvp_edge"), IBM_POS("IBM_POS",
			"ibm_hashed_timeline_sentiment_pos_jan_aug",
			"ibm_hashed_timeline_sentiment_pos_sep_dec", "sender_id",
			"receiver_id", "pos", ""), IBM_NEG("IBM_NEG",
			"ibm_hashed_timeline_sentiment_neg_jan_aug",
			"ibm_hashed_timeline_sentiment_neg_sep_dec", "sender_id",
			"receiver_id", "neg", ""), CR3_FRIEND("FRIEND",
			"cr3_friend_may_jul", "cr3_friend_aug_sep", "src_char_id",
			"dest_char_id", "friend", "lp_friend_edge"), CR3_MENTOR("MENTOR",
			"cr3_mentor_may_jul", "cr3_mentor_aug_sep", "src_char_id",
			"dest_char_id", "mentor", "lp_mentoring_edge"), CR3_TEAM("TEAM",
			"cr3_team_may_jul", "cr3_team_aug_sep", "src_char_id",
			"dest_char_id", "team", "lp_team_edge"), EPINION_POS("EPINION_POS",
			"ep_pos_training", "ep_pos_test", "src_user_id", "dest_user_id",
			"pos", ""), EPINION_NEG("EPINION_NEG", "ep_neg_training",
			"ep_neg_test", "src_user_id", "dest_user_id", "neg", "");

	private String name;

	private String trainingPeriodTable;

	private String testPeriodTable;

	private String charOneColumn;

	private String charTwoColumn;

	private String featurePrefix;

	private String datasetTable;

	private LPNetwork(String name, String trainingPeriodTable,
			String testPeriodTable, String charOneColumn, String charTwoColumn,
			String featurePrefix, String datasetTable) {
		this.name = name;
		this.trainingPeriodTable = trainingPeriodTable;
		this.testPeriodTable = testPeriodTable;
		this.charOneColumn = charOneColumn;
		this.charTwoColumn = charTwoColumn;
		this.featurePrefix = featurePrefix;
		this.datasetTable = datasetTable;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getTrainingPeriodTable() {
		return trainingPeriodTable;
	}

	public void setTrainingPeriodTable(String trainingPeriodTable) {
		this.trainingPeriodTable = trainingPeriodTable;
	}

	public String getTestPeriodTable() {
		return testPeriodTable;
	}

	public void setTestPeriodTable(String testPeriodTable) {
		this.testPeriodTable = testPeriodTable;
	}

	public String getCharOneColumn() {
		return charOneColumn;
	}

	public void setCharOneColumn(String charOneColumn) {
		this.charOneColumn = charOneColumn;
	}

	public String getCharTwoColumn() {
		return charTwoColumn;
	}

	public void setCharTwoColumn(String charTwoColumn) {
		this.charTwoColumn = charTwoColumn;
	}

	public String getFeaturePrefix() {
		return featurePrefix;
	}

	public void setFeaturePrefix(String featurePrefix) {
		this.featurePrefix = featurePrefix;
	}

	public String getDatasetTable() {
		return datasetTable;
	}

	public void setDatasetTable(String datasetTable) {
		this.datasetTable = datasetTable;
	}
}
