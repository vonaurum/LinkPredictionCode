package linkpred.superlearn.bbn;

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

public class BBNExtSupervisedLearning {

	private static int NUM_POSITIVES = 1103;
	private static int NUM_NEGATIVES = 58897;

	private static final String DATASET_FILE_PATH_PREFIX = "C:\\Users\\zborbor\\work\\linkpred\\bbn\\";

	private static final String RESULTS_FILE_NAME = "C:\\Users\\zborbor\\work\\linkpred\\bbn\\bbn_results.csv";

	private static final String DATASET = "dataset_ext1";

	private static BBNExtSupervisedLearning _instance = new BBNExtSupervisedLearning();

	private BBNExtSupervisedLearning() {
	}

	public static BBNExtSupervisedLearning getInstance() {
		return _instance;
	}

	public static void main(String[] args) {
		getInstance().run();
	}

	/**
	 * <pre>
	 * </pre>
	 */
	public void run() {

		BBNExtFeatureSetConstructor bbnExtFeatureSetConstructor = new BBNExtFeatureSetConstructor();
		try {

			bbnExtFeatureSetConstructor.loadJUNGGraph();

			System.out.println("Step 5.2.4 - Calculate degree centrality");
			// bbnExtFeatureSetConstructor.computePlayerDegreeCentrality();
			System.out.println("Step 5.2.5 - Calculate betweenness centrality");
			// bbnExtFeatureSetConstructor.computePlayerBetweennessCentrality();
			System.out.println("Step 5.2.6 - Calculate closeness centrality");
			bbnExtFeatureSetConstructor.computePlayerClosenessCentrality();
			System.out.println("Step 5.2.7 - Calculate eigenvector centrality");
			// bbnExtFeatureSetConstructor.computeEigenvectorCentrality();

			// writeDatasetToFile();
			// runClassifiers(NUM_POSITIVES, NUM_NEGATIVES);

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("Done!!");
	}

	private void writeDatasetToFile() {

		System.out.println("Step 6 - Writing dataset to file");
		Connection conn_src = null;
		BufferedWriter bufferedWriter = null;
		try {

			conn_src = ConnectionUtil.getGUILEConnection();
			Statement stmt = conn_src.createStatement();

			System.out.println("********** Start: Fetch data");
			ResultSet rs = stmt
					.executeQuery("SELECT * FROM bbn_training_period_pairs");
			System.out.println("********** End: Fetch data");

			if (rs != null) {

				bufferedWriter = new BufferedWriter(new FileWriter(
						DATASET_FILE_PATH_PREFIX + DATASET + ".csv"));

				bufferedWriter
						.write("player1_char_id, player2_char_id, real_gender_indicator, real_country_indicator, "
								+ "game_class_indicator, game_gender_indicator, game_race_indicator, "
								+ "sum_age, diff_age, sum_joining_age, diff_joining_age, sum_char_level, "
								+ " sum_neighbors, shortest_ditsance, sum_clustering_index, "
								+ "common_neighbors, aa_index, ra_index, form_link");
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
							+ rs.getInt("diff_age") + ","
							+ rs.getInt("sum_joining_age") + ","
							+ rs.getInt("diff_joining_age") + ","
							+ rs.getInt("sum_char_level") + ","
							+ rs.getInt("sum_neighbors") + ","
							+ rs.getInt("shortest_ditsance") + ","
							+ rs.getFloat("sum_clustering_index") + ","
							+ rs.getInt("common_neighbors") + ","
							+ rs.getFloat("aa_index") + ","
							+ rs.getFloat("ra_index") + ","
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

	private void runClassifiers(int numPositive, int numNegative) {

		System.out.println("Step 7 - Running classifiers");

		String trainingQuery = "select real_gender_indicator, real_country_indicator, game_class_indicator, game_gender_indicator, "
				+ "game_race_indicator, sum_age, diff_age, sum_joining_age, diff_joining_age, sum_char_level, "
				+ "sum_neighbors, shortest_ditsance, sum_clustering_index, common_neighbors, "
				+ "aa_index, ra_index, form_link from bbn_training_period_pairs";

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

		bufferedWriter.write(DATASET + ","
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