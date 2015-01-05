package linkpred.superlearn.bbn;

import java.sql.SQLException;

public class BBNSupervisedLearning {

	private static int MAX_TOTAL_SAMPLE_SIZE = 60000;

	private static int MAX_POSITIVE_SAMPLE_SIZE = 29999;

	private static BBNSupervisedLearning _instance = new BBNSupervisedLearning();

	private BBNSupervisedLearning() {
	}

	public static BBNSupervisedLearning getInstance() {
		return _instance;
	}

	/**
	 * <pre>
	 * 1. trust -> group: done
	 * 2. group -> trust: done
	 * 3. mentoring -> trust: done
	 * 4. trust -> mentoring:
	 * </pre>
	 */
	public static void main(String[] args) {
		getInstance().run();
	}

	public void run() {

		System.out.println("Building data.. ");
		buildData();
		System.out.println("Constructing features.. ");
		constructFeatures();
		System.out.println("Done!!");
	}

	public void buildData() {

		System.out
				.println("Step 1 - construct players pairs in test period with link "
						+ "between them");
		new BBNTestDataProcessor().rebuildPlayerEdges();

		System.out
				.println("Step 2 - construct players pairs in training period with link "
						+ "between them. These should never be used as positive samples.");
		new BBNTrainingDataProcessor().rebuildPlayerEdges();

		System.out
				.println("Step 3 - build individual player data for training period");
		new BBNTrainingDataProcessor().rebuildPlayerData();

		System.out.println("Step 4 - construct edge samples. Sample size = "
				+ MAX_TOTAL_SAMPLE_SIZE);

		new BBNTrainingDataProcessor().deleteAllExistingEdgeSamples();
		System.out.println("Deleted all edge samples");
		System.out
				.println("Step 4.1 - construct positive samples by using data from "
						+ "previous steps. Max positve samples = "
						+ MAX_POSITIVE_SAMPLE_SIZE);

		int numPositive = new BBNTrainingDataProcessor()
				.buildPositiveEdgeSamples(MAX_POSITIVE_SAMPLE_SIZE);
		System.out
				.println("No. of training pairs inserted as positive samples = "
						+ numPositive);

		System.out.println("Step 4.2 - constructing negative samples..");
		int numNegative = new BBNTrainingDataProcessor()
				.buildNegativeEdgeSamples(MAX_TOTAL_SAMPLE_SIZE);
		System.out
				.println("No. of training pairs inserted as negative samples = "
						+ numNegative);
	}

	public void constructFeatures() {

		System.out
				.println("Step 5 - Construct features of the pairs created in Step 4.");
		System.out.println("Step 5.1 - Construct node features..");
		constructNodeFeatures();
		System.out.println("Step 5.2 - Constructing network features..");
		constructNetworkFeatures();
		System.out
				.println("Step 5.3 - Constructing weighted network features..");
		constructWeightedNetworkFeatures();
	}

	private void constructNodeFeatures() {
		new BBNFeatureSetConstructor().constructFeatureSet(1, false);
	}

	private void constructNetworkFeatures() {

		System.out
				.println("Step 5.2.1 - Calculating unweighted edge measures - sum degree, diff degree, common neighbors, aa_index, ra_index.. ");
		new BBNFeatureSetConstructor().constructFeatureSet(4, false);
		System.out
				.println("Step 5.2.2 - Calculating shortest distance between pair "
						+ "(Dijkstra's ).. ");
		new BBNFeatureSetConstructor().constructFeatureSet(2, true);
		System.out.println("Step 5.2.3 - Calculating clustering index.. ");
		new BBNFeatureSetConstructor().computePlayerClusteringIndex();
		new BBNFeatureSetConstructor().constructFeatureSet(3, true);
		System.out.println("Step 5.2.4 - Calculating centrality measures.. ");
		computePlayerCentralityMeasures();
		new BBNFeatureSetConstructor().constructFeatureSet(8, false);
	}

	private void computePlayerCentralityMeasures() {

		BBNExtFeatureSetConstructor bbnExtFeatureSetConstructor = new BBNExtFeatureSetConstructor();
		try {

			bbnExtFeatureSetConstructor.loadJUNGGraph();

			System.out
					.println("Step 5.2.4.1 - Calculating degree centrality.. ");
			bbnExtFeatureSetConstructor.computePlayerDegreeCentrality();
			System.out
					.println("Step 5.2.4.2 - Calculating betweenness centrality.. ");
			bbnExtFeatureSetConstructor.computePlayerBetweennessCentrality();
			System.out
					.println("Step 5.2.4.3 - Calculating closeness centrality.. ");
			// bbnExtFeatureSetConstructor.computePlayerClosenessCentrality();
			System.out
					.println("Step 5.2.4.4 - Calculating eigenvector centrality.. ");
			bbnExtFeatureSetConstructor.computePlayerEigenvectorCentrality();

		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	private void constructWeightedNetworkFeatures() {

		System.out.println("Step 5.3.1 - Calculating WCN index..");
		new BBNFeatureSetConstructor().constructFeatureSet(5, false);
		System.out.println("Step 5.3.2 - Calculating WAA index..");
		new BBNFeatureSetConstructor().constructFeatureSet(6, false);
		System.out.println("Step 5.3.3 - Calculating WRA index..");
		new BBNFeatureSetConstructor().constructFeatureSet(7, false);
	}
}