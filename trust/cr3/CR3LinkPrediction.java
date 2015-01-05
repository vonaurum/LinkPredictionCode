package linkpred.trust.cr3;

import linkpred.trust.LPExperiment;
import linkpred.trust.LoadPlayerNetwork;
import linkpred.trust.bean.LPNetwork;
import etl.bean.Month;

public class CR3LinkPrediction {

	private static int MAX_TOTAL_SAMPLE_SIZE = 60000;

	private static int MAX_POSITIVE_SAMPLE_SIZE = 30000;

	private static CR3LinkPrediction _instance = new CR3LinkPrediction();

	private CR3LinkPrediction() {
	}

	public static CR3LinkPrediction getInstance() {
		return _instance;
	}

	/**
	 * Start: Analysis parameters
	 */

	private static final int SERVER_ID = 0;

	private static final LPNetwork TRAINING_NETWORK = LPNetwork.CR3_MENTOR;

	private static final LPNetwork TEST_NETWORK = LPNetwork.CR3_MENTOR;

	private static final Month TRAINING_START_MONTH = Month.MAY;

	private static final Month TRAINING_END_MONTH = Month.JUL;

	private static final String TRAINING_PERIOD = Month.getMonthsRange(
			TRAINING_START_MONTH, TRAINING_END_MONTH);

	private static final String TEST_PERIOD = Month.getMonthsRange(Month.AUG,
			Month.SEP);

	// also make sure table names and columns are correct in LPNetwork enum
	/**
	 * End: Analysis parameters
	 */

	/**
	 * 
	 * <pre>
	 * CR3:
	 * 	1. Run network features for FRIEND: done (except betweenness, closeness, eigenvector)
	 * 	2. Run network features for MENTOR: done (except betweenness, closeness, eigenvector)
	 * 	3. Run network features for GROUP: done (except betweenness, closeness, eigenvector)
	 * 
	 *  CR3
	 *  FF: done, MF: done, TF: done, FM: done, MM: done, TM: , FT: , MT: , GT:
	 * 
	 * </pre>
	 */
	public static void main(String[] args) {
		getInstance().run();
	}

	public void run1() {

		LoadCR3EdgeDataset loadEdgeDataset = new LoadCR3EdgeDataset();
		System.out
				.println("Step 6.3 "
						+ getIdString()
						+ " - Constructing features based on cross-network properties..");
		loadEdgeDataset.loadFeatures(3, TRAINING_NETWORK, TRAINING_PERIOD,
				SERVER_ID);
	}

	public void run() {

		System.out.println("********** Building node features.. ");
		// loadNodeFeatures();
		System.out.println("********** Constructing dataset.. ");
		loadDataset();
		System.out.println("********** Run experiments.. ");
		new LPExperiment().run();
		System.out.println("********** Final complete!!");
	}

	public void loadNodeFeatures() {
		loadStaticFeatures();
		loadDynamicIndividualFeatures();
		loadDynamicNetworkFeatures();
	}

	private void loadStaticFeatures() {

		LoadPlayerFeatures loadPlayerFeatures = new LoadPlayerFeatures();
		System.out
				.println("Step 1 "
						+ getIdString()
						+ " - Build static features of player nodes in training period.. ");
		loadPlayerFeatures.loadPlayerStaticFeatures(SERVER_ID, TRAINING_PERIOD);
	}

	private void loadDynamicIndividualFeatures() {

		LoadPlayerFeatures loadPlayerFeatures = new LoadPlayerFeatures();
		System.out
				.println("Step 2 "
						+ getIdString()
						+ " - Build time-based individual features of player nodes in training period.. ");
		loadPlayerFeatures.loadPlayerDynamicIndividualFeatures(
				TRAINING_START_MONTH, TRAINING_END_MONTH, SERVER_ID);
	}

	private void loadDynamicNetworkFeatures() {

		System.out
				.println("Step 3 "
						+ getIdString()
						+ " - Build time-based network features of player nodes in training period.. ");
		System.out.println("Step 3.1 " + getIdString()
				+ " - Rebuilding player training network.. ");
		LoadPlayerNetwork loadPlayerNetwork = new LoadPlayerNetwork();
		loadPlayerNetwork.rebuildPlayerNetwork(TRAINING_NETWORK,
				"lp_training_period_edge");
		System.out.println("Step 3.2 " + getIdString()
				+ " - Populating time-based network features.. ");

		LoadPlayerNetworkFeatures loadPlayerNetworkFeatures = new LoadPlayerNetworkFeatures();
		System.out.println("Step 3.2.1 " + getIdString()
				+ " - Populating player neighbors.. ");
		loadPlayerNetworkFeatures.loadPlayerNeighbors(TRAINING_NETWORK,
				TRAINING_PERIOD, SERVER_ID);
		System.out.println("Step 3.2.2 " + getIdString()
				+ " - Populating player clustering index.. ");
		loadPlayerNetworkFeatures.loadPlayerClusteringIndex(TRAINING_NETWORK,
				TRAINING_PERIOD, SERVER_ID);
		System.out.println("Step 3.2.3 " + getIdString()
				+ " - Populating player degree centrality.. ");
		loadPlayerNetworkFeatures.loadPlayerDegreeCentrality(TRAINING_NETWORK,
				TRAINING_PERIOD, SERVER_ID);
		/*
		 * System.out.println("Step 3.2.4 " + getIdString() +
		 * " - Populating player betweenness centrality.. ");
		 * loadPlayerNetworkFeatures.loadPlayerBetweennessCentrality(
		 * TRAINING_NETWORK, TRAINING_PERIOD, SERVER_ID);
		 */

		/*
		 * System.out.println("Step 3.2.5 " + getIdString() +
		 * " - Populating player closeness centrality.. ");
		 * loadPlayerNetworkFeatures.loadPlayerClosenessCentrality(
		 * TRAINING_NETWORK, TRAINING_PERIOD, SERVER_ID);
		 * System.out.println("Step 3.2.6 " + getIdString() +
		 * " - Populating player eigenvector centrality.. ");
		 * loadPlayerNetworkFeatures.loadPlayerEigenvectorCentrality(
		 * TRAINING_NETWORK, TRAINING_PERIOD, SERVER_ID);
		 */
	}

	public void loadDataset() {
		loadNetworks();
		loadEdgeSamples();
		loadEdgeFeatures();
	}

	private void loadNetworks() {

		System.out
				.println("Step 4 " + getIdString() + " - Rebuild networks.. ");
		LoadPlayerNetwork loadPlayerNetwork = new LoadPlayerNetwork();
		System.out.println("Step 4.1 " + getIdString()
				+ " - Rebuilding player training network.. ");
		loadPlayerNetwork.rebuildPlayerNetwork(TRAINING_NETWORK,
				"lp_training_period_edge");
		System.out.println("Step 4.2 " + getIdString()
				+ " - Rebuilding player test network.. ");
		loadPlayerNetwork.rebuildPlayerNetwork(TEST_NETWORK,
				"lp_test_period_edge");
	}

	private void loadEdgeSamples() {

		System.out.println("Step 5 " + getIdString()
				+ " - Constructing edge samples ( total sample size = "
				+ MAX_TOTAL_SAMPLE_SIZE + ")..");

		LoadCR3EdgeDataset loadEdgeDataset = new LoadCR3EdgeDataset();
		System.out.println("Step 5.1 " + getIdString()
				+ " - Deleting existing edges samples..");
		loadEdgeDataset.deleteAllExistingEdgeSamples();
		System.out.println("Step 5.2 " + getIdString()
				+ " - Constructing positive samples (max samples = "
				+ MAX_POSITIVE_SAMPLE_SIZE + ")..");
		int numPositive = loadEdgeDataset.buildPositiveEdgeSamples(
				MAX_POSITIVE_SAMPLE_SIZE, TRAINING_PERIOD, SERVER_ID);
		System.out.println("Step 5.3 " + getIdString()
				+ " - Constructing negative samples..");
		int numNegative = loadEdgeDataset.buildNegativeEdgeSamples(
				MAX_TOTAL_SAMPLE_SIZE, TRAINING_PERIOD, SERVER_ID);
		System.out.println("Postive:negative ratio = " + numPositive + ":"
				+ numNegative);

	}

	private void loadEdgeFeatures() {

		System.out.println("Step 6 " + getIdString()
				+ " - Constructing features of edge samples..");

		LoadCR3EdgeDataset loadEdgeDataset = new LoadCR3EdgeDataset();
		System.out.println("Step 6.1 " + getIdString()
				+ " - Constructing features based on node properties..");
		loadEdgeDataset.loadFeatures(1, TRAINING_NETWORK, TRAINING_PERIOD,
				SERVER_ID);
		System.out.println("Step 6.2 " + getIdString()
				+ " - Constructing features based on topological properties..");
		loadEdgeDataset.loadFeatures(2, TRAINING_NETWORK, TRAINING_PERIOD,
				SERVER_ID);
		System.out
				.println("Step 6.3 "
						+ getIdString()
						+ " - Constructing features based on cross-network properties..");
		loadEdgeDataset.loadFeatures(3, TRAINING_NETWORK, TRAINING_PERIOD,
				SERVER_ID);
		System.out.println("Step 6.4 " + getIdString()
				+ " - Constructing weighted topological features..");

		// constructWeightedNetworkFeatures();
	}

	private String getIdString() {
		return "<" + TRAINING_NETWORK.getName() + "-" + TEST_NETWORK.getName()
				+ ", server:" + SERVER_ID + ", " + TRAINING_PERIOD + ">";
	}
}