package linkpred.trust.epinion;

import linkpred.trust.LPExperiment;
import linkpred.trust.LoadPlayerNetwork;
import linkpred.trust.bean.LPNetwork;

public class EpinionLinkPrediction {

	private static int MAX_TOTAL_SAMPLE_SIZE = 60000;

	private static int MAX_POSITIVE_SAMPLE_SIZE = 30000;

	private static EpinionLinkPrediction _instance = new EpinionLinkPrediction();

	private EpinionLinkPrediction() {
	}

	public static EpinionLinkPrediction getInstance() {
		return _instance;
	}

	/**
	 * Start: Analysis parameters
	 */

	private static final LPNetwork TRAINING_NETWORK = LPNetwork.EPINION_NEG;

	private static final LPNetwork TEST_NETWORK = LPNetwork.EPINION_NEG;

	private static final String TRAINING_PERIOD = "training";

	// also make sure table names and columns are correct in LPNetwork enum
	/**
	 * End: Analysis parameters
	 */

	/**
	 * 
	 * <pre>
	 * 	1. Run network features for POS: 
	 * 	2. Run network features for NEG:
	 * 
	 * </pre>
	 */
	public static void main(String[] args) {
		getInstance().run();
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
		loadNodes();
		loadDynamicNetworkFeatures();
	}

	private void loadNodes() {

		LoadEpinionNodeFeatures loadNodeFeatures = new LoadEpinionNodeFeatures();
		System.out
				.println("Step 1 "
						+ getIdString()
						+ " - Build static features of player nodes in training period.. ");
		loadNodeFeatures.loadTrainingNodes();
	}

	private void loadDynamicNetworkFeatures() {

		System.out
				.println("Step 2 "
						+ getIdString()
						+ " - Build time-based network features of player nodes in training period.. ");
		System.out.println("Step 2.1 " + getIdString()
				+ " - Rebuilding player training network.. ");
		LoadPlayerNetwork loadPlayerNetwork = new LoadPlayerNetwork();
		loadPlayerNetwork.rebuildPlayerNetwork(TRAINING_NETWORK,
				"lp_training_period_edge");
		System.out.println("Step 2.2 " + getIdString()
				+ " - Populating time-based network features.. ");
		LoadEpinionNodeFeatures loadNodeFeatures = new LoadEpinionNodeFeatures();
		System.out.println("Step 2.2.1 " + getIdString()
				+ " - Populating player neighbors.. ");
		loadNodeFeatures.loadPlayerNeighbors(TRAINING_NETWORK, TRAINING_PERIOD);
		System.out.println("Step 2.2.2 " + getIdString()
				+ " - Populating player clustering index.. ");
		loadNodeFeatures.loadPlayerClusteringIndex(TRAINING_NETWORK,
				TRAINING_PERIOD);
		System.out.println("Step 2.2.3 " + getIdString()
				+ " - Populating player degree centrality.. ");
		loadNodeFeatures.loadPlayerDegreeCentrality(TRAINING_NETWORK,
				TRAINING_PERIOD);
		System.out.println("Step 2.2.4 " + getIdString()
				+ " - Populating player betweenness centrality.. ");
		loadNodeFeatures.loadPlayerBetweennessCentrality(TRAINING_NETWORK,
				TRAINING_PERIOD);
		System.out.println("Step 2.2.5 " + getIdString()
				+ " - Populating player closeness centrality.. ");
		loadNodeFeatures.loadPlayerClosenessCentrality(TRAINING_NETWORK,
				TRAINING_PERIOD);
		System.out.println("Step 2.2.6 " + getIdString()
				+ " - Populating player eigenvector centrality.. ");
		loadNodeFeatures.loadPlayerEigenvectorCentrality(TRAINING_NETWORK,
				TRAINING_PERIOD);
	}

	public void loadDataset() {
		loadNetworks();
		loadEdgeSamples();
		loadEdgeFeatures();
	}

	private void loadNetworks() {

		System.out
				.println("Step 3 " + getIdString() + " - Rebuild networks.. ");
		LoadPlayerNetwork loadPlayerNetwork = new LoadPlayerNetwork();
		System.out.println("Step 3.1 " + getIdString()
				+ " - Rebuilding player training network.. ");
		loadPlayerNetwork.rebuildPlayerNetwork(TRAINING_NETWORK,
				"lp_training_period_edge");
		System.out.println("Step 3.2 " + getIdString()
				+ " - Rebuilding player test network.. ");
		loadPlayerNetwork.rebuildPlayerNetwork(TEST_NETWORK,
				"lp_test_period_edge");
	}

	private void loadEdgeSamples() {

		System.out.println("Step 4 " + getIdString()
				+ " - Constructing edge samples ( total sample size = "
				+ MAX_TOTAL_SAMPLE_SIZE + ")..");
		LoadEpinionEdgeDataset loadEdgeDataset = new LoadEpinionEdgeDataset();
		System.out.println("Step 4.1 " + getIdString()
				+ " - Deleting existing edges samples..");
		loadEdgeDataset.deleteAllExistingEdgeSamples();
		System.out.println("Step 4.2 " + getIdString()
				+ " - Constructing positive samples (max samples = "
				+ MAX_POSITIVE_SAMPLE_SIZE + ")..");
		int numPositive = loadEdgeDataset.buildPositiveEdgeSamples(
				MAX_POSITIVE_SAMPLE_SIZE, TRAINING_PERIOD, 0);
		System.out.println("Step 4.3 " + getIdString()
				+ " - Constructing negative samples..");
		int numNegative = loadEdgeDataset.buildNegativeEdgeSamples(
				MAX_TOTAL_SAMPLE_SIZE, TRAINING_PERIOD, 0);
		System.out.println("Postive:negative ratio = " + numPositive + ":"
				+ numNegative);
	}

	private void loadEdgeFeatures() {

		System.out.println("Step 5 " + getIdString()
				+ " - Constructing features of edge samples..");
		LoadEpinionEdgeDataset loadEdgeDataset = new LoadEpinionEdgeDataset();
		System.out.println("Step 5.1 " + getIdString()
				+ " - Constructing features based on topological properties..");
		loadEdgeDataset.loadFeatures(2, TRAINING_NETWORK, TRAINING_PERIOD, 0);
		System.out
				.println("Step 5.2 "
						+ getIdString()
						+ " - Constructing features based on cross-network properties..");
		loadEdgeDataset.loadFeatures(3, TRAINING_NETWORK, TRAINING_PERIOD, 0);
		System.out.println("Step 5.3 " + getIdString()
				+ " - Constructing weighted topological features.."); // //
		// constructWeightedNetworkFeatures();
	}

	private String getIdString() {
		return "<" + TRAINING_NETWORK.getName() + "-" + TEST_NETWORK.getName()
				+ ", " + TRAINING_PERIOD + ">";
	}
}