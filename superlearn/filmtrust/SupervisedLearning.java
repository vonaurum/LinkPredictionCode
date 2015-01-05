package linkpred.superlearn.filmtrust;

public class SupervisedLearning {

	private String FT_TRAINING_EDGE = "ft_training_edge";

	private String FT_TEST_EDGE = "ft_test_edge";

	private static SupervisedLearning _instance = new SupervisedLearning();

	private SupervisedLearning() {
	}

	public static SupervisedLearning getInstance() {
		return _instance;
	}

	public static void main(String[] args) {
		getInstance().run();
	}

	public void run() {

		System.out
				.println("Populating positve samples in training and test set");
		// new DataProcessor().rebuildEdgeSamples();

		System.out.println("Constructing features of training data");
		// constructFeatures(FT_TRAINING_EDGE);

		System.out.println("Constructing features of test data");
		constructFeatures(FT_TEST_EDGE);

		System.out.println("Done!!");
	}

	private void constructFeatures(String tableName) {

		FeatureSetConstructor featureSetConstructor = new FeatureSetConstructor(
				tableName);

		System.out.println(tableName
				+ ": Calculate shortest distance between pair (Dijkstra's )");
		// featureSetConstructor.constructFeatureSet(2, true);
		if (FT_TEST_EDGE.equals(tableName)) {
			featureSetConstructor.constructFeatureSet(21, true);
		}

		System.out.println(tableName + ": Calculate clustering index");
		// featureSetConstructor.constructFeatureSet(3, true);

		System.out.println(tableName
				+ ": Calculate unweighted measures between pair ");
		// featureSetConstructor.constructFeatureSet(4, true);

		/*
		 * System.out.println(tableName + ": Calculate WCN index of pairs");
		 * featureSetConstructor.constructFeatureSet(5, true);
		 * 
		 * System.out .println(tableName + ": Calculate WAA index of pairs ");
		 * featureSetConstructor.constructFeatureSet(6, true);
		 * 
		 * System.out .println(tableName + ": Calculate WRA index of pairs ");
		 * featureSetConstructor.constructFeatureSet(7, true);
		 */
	}
}
