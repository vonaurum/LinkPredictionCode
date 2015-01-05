package linkpred.superlearn.generic;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import util.ConnectionUtil;
import weka.EvalClassifer;
import weka.bean.ModelResults;
import weka.bean.WekaClassifier;

public class SupervisedLearning {

	private static int MAX_TOTAL_SAMPLE_SIZE = 60000;
	private static int MAX_POSITIVE_SAMPLE_SIZE = 29999;

	private static final String DATASET_FILE_PATH_PREFIX = "C:\\Users\\zborbor\\work\\linkpred\\dataset\\";

	private static final String RESULTS_FILE_NAME = "C:\\Users\\zborbor\\work\\linkpred\\results.csv";

	private static final String TRAINING_NETWORK = "trust";

	private static final String TEST_NETWORK = "group";

	private static SupervisedLearning _instance = new SupervisedLearning();

	private SupervisedLearning() {
	}

	public static SupervisedLearning getInstance() {
		return _instance;
	}

	public static void main(String[] args) {
		getInstance().run();
	}

	private void run1() {

	}

	/**
	 * <pre>
	 * 1. housing -> mentoring : done
	 * 2. mentoring -> housing : done
	 * 3. mentoring -> trading : done
	 * 4. trading -> mentoring : done
	 * 5. housing -> trading : done
	 * 6. trading -> housing : done
	 * 7. PvP -> PvP : done
	 * 8. trust_ng -> trust_ng : no positive samples!!
	 * 9. group_guk_acnt -> group_guk_acnt : done
	 * 10. group -> group : done
	 * 11. group -> trust : done
	 * 12. group -> trade : done
	 * 13. pvp -> trade : done
	 * 14. trade -> pvp : done
	 * 15. mentoring -> group:
	 * 16. group -> mentoring:
	 * 17. mentoring -> PvP:
	 * </pre>
	 */
	public void run() {

		System.out
				.println("Step 1 - construct players pairs in test period with link "
						+ "between them");
		new TestDataProcessor().rebuildPlayerEdges();

		System.out
				.println("Step 2 - construct players pairs in training period with link "
						+ "between them. These should never be used as positive samples.");
		// TODO: uncomment when training set changes
		new TrainingDataProcessor().rebuildPlayerEdges();

		System.out
				.println("Step 3 - build individual player data for training period");
		// TODO: uncomment when training set changes
		new TrainingDataProcessor().rebuildPlayerData();

		System.out.println("Step 4 - construct edge samples. Sample size = "
				+ MAX_TOTAL_SAMPLE_SIZE);

		new TrainingDataProcessor().deleteAllExistingEdgeSamples();
		System.out.println("Deleted all edge samples");
		System.out
				.println("Step 4.1 - construct positive samples by using data from "
						+ "previous steps. Max positve samples = "
						+ MAX_POSITIVE_SAMPLE_SIZE);

		int numPositive = new TrainingDataProcessor()
				.buildPositiveEdgeSamples(MAX_POSITIVE_SAMPLE_SIZE);
		System.out
				.println("No. of training pairs inserted as positive samples = "
						+ numPositive);

		System.out.println("Step 4.2 - constructing negative samples..");
		int numNegative = new TrainingDataProcessor()
				.buildNegativeEdgeSamples(MAX_TOTAL_SAMPLE_SIZE);
		System.out
				.println("No. of training pairs inserted as negative samples = "
						+ numNegative);

		System.out
				.println("Step 5 - Construct features of the pairs created in Step 4.");
		System.out
				.println("Step 5.1 - Construct proximity and aggregated features");
		new FeatureSetConstructor().constructFeatureSet(1, false);
		System.out.println("Step 5.2 - Construct network topology features");
		System.out
				.println("Step 5.2.1 - Calculate shortest distance between pair "
						+ "(Dijkstra's )");
		new FeatureSetConstructor().constructFeatureSet(2, true);
		System.out.println("Step 5.2.2 - Calculate clustering index");
		// TODO: uncomment when training set changes
		new FeatureSetConstructor().computePlayerClusteringIndex();
		new FeatureSetConstructor().constructFeatureSet(3, true);

		System.out.println("Step 6 - Writing dataset to file");
		writeDatasetToFile();

		System.out.println("Step 7 - Running classifiers");
		String trainingQuery = "select real_gender_indicator, real_country_indicator, game_class_indicator, "
				+ "game_gender_indicator, game_race_indicator, sum_age, sum_joining_age, sum_char_level, "
				+ "sum_neighbors, shortest_ditsance, sum_clustering_index, form_link from training_period_pairs";

		BufferedWriter bufferedWriter = null;
		try {
			bufferedWriter = new BufferedWriter(new FileWriter(
					RESULTS_FILE_NAME, true));
			File file = new File(RESULTS_FILE_NAME);
			if (!file.exists()) {
				bufferedWriter
						.write("training network, test network, classifier, # positives, # negatives, PrecisionY, RecallY, F-measureY");
				bufferedWriter.newLine();
			}

			System.out.println("Step 7.1 - Running "
					+ WekaClassifier.J48.getDescription());
			runClassifier(WekaClassifier.J48, trainingQuery, numPositive,
					numNegative, bufferedWriter);

			System.out.println("Step 7.2 - Running "
					+ WekaClassifier.JRIP.getDescription());
			runClassifier(WekaClassifier.JRIP, trainingQuery, numPositive,
					numNegative, bufferedWriter);

			System.out.println("Step 7.3 - Running "
					+ WekaClassifier.NAIVE_BAYES.getDescription());
			runClassifier(WekaClassifier.NAIVE_BAYES, trainingQuery,
					numPositive, numNegative, bufferedWriter);

			System.out.println("Step 7.4 - Running "
					+ WekaClassifier.BAYES_NET.getDescription());
			runClassifier(WekaClassifier.BAYES_NET, trainingQuery, numPositive,
					numNegative, bufferedWriter);

			System.out.println("Step 7.5 - Running "
					+ WekaClassifier.ADA_BOOST.getDescription());
			runClassifier(WekaClassifier.ADA_BOOST, trainingQuery, numPositive,
					numNegative, bufferedWriter);

			System.out.println("Step 7.6 - Running "
					+ WekaClassifier.IBK_3.getDescription());
			runClassifier(WekaClassifier.IBK_3, trainingQuery, numPositive,
					numNegative, bufferedWriter);

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				bufferedWriter.flush();
				bufferedWriter.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		System.out.println("Done!!");
	}

	private void writeDatasetToFile() {

		Connection conn_src = null;
		BufferedWriter bufferedWriter = null;
		try {

			conn_src = ConnectionUtil.getGUILEConnection();
			Statement stmt = conn_src.createStatement();

			System.out.println("********** Start: Fetch data");
			ResultSet rs = stmt
					.executeQuery("SELECT * FROM training_period_pairs");
			System.out.println("********** End: Fetch data");

			if (rs != null) {

				bufferedWriter = new BufferedWriter(new FileWriter(
						DATASET_FILE_PATH_PREFIX + TRAINING_NETWORK + "_"
								+ TEST_NETWORK + ".csv"));

				bufferedWriter
						.write("player1_char_id,player2_char_id,real_gender_indicator,real_country_indicator, "
								+ "game_class_indicator,game_gender_indicator,game_race_indicator,sum_age,sum_joining_age,"
								+ "sum_char_level,sum_neighbors,shortest_ditsance,sum_clustering_index,form_link");
				bufferedWriter.newLine();
				while (rs.next()) {

					bufferedWriter.write(rs.getInt("player1_char_id") + ","
							+ rs.getInt("player2_char_id") + ","
							+ rs.getInt("real_gender_indicator") + ","
							+ rs.getInt("real_country_indicator") + ","
							+ rs.getInt("game_class_indicator") + ","
							+ rs.getInt("game_gender_indicator") + ","
							+ rs.getInt("game_race_indicator") + ","
							+ rs.getInt("sum_age") + ","
							+ rs.getInt("sum_joining_age") + ","
							+ rs.getInt("sum_char_level") + ","
							+ rs.getInt("sum_neighbors") + ","
							+ rs.getInt("shortest_ditsance") + ","
							+ rs.getFloat("sum_clustering_index") + ","
							+ rs.getString("form_link"));
					bufferedWriter.newLine();
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				bufferedWriter.flush();
				bufferedWriter.close();
				conn_src.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		System.out.println("Finished writing data set to file");
	}

	private void runClassifier(WekaClassifier classifier, String trainingQuery,
			int numPositive, int numNegative, BufferedWriter bufferedWriter)
			throws IOException {

		ModelResults results = new EvalClassifer(trainingQuery)
				.evaluateModel(classifier);
		results.setClassifier(classifier);
		results.setNumY(numPositive);
		results.setNumN(numNegative);
		printResults(results);
		appendResultsToFile(results, bufferedWriter);
	}

	private void appendResultsToFile(ModelResults results,
			BufferedWriter bufferedWriter) throws IOException {

		bufferedWriter.write(TRAINING_NETWORK + "," + TEST_NETWORK + ","
				+ results.getClassifier().getDescription() + ","
				+ results.getNumY() + "," + results.getNumN() + ","
				+ results.getPrecisionY() + "," + results.getRecallY() + ","
				+ results.getfMeasureY());
		bufferedWriter.newLine();
	}

	private void printResults(ModelResults results) {

		System.out.println("Confusion matrix ");
		System.out.println("TP = " + results.getTruePositive() + "\tFN = "
				+ results.getFalseNegative());
		System.out.println("FP = " + results.getFalsePositive() + "\tTN= "
				+ results.getTrueNegative());
		System.out.println("Precision " + results.getPrecisionY() + "\tRecall "
				+ results.getRecallY() + "\tF-measure "
				+ results.getfMeasureY());
	}
}