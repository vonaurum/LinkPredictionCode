/*
 * BBNExperiment.java
 *
 * Created on Jul 1, 2010 
 */
package linkpred.trust;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Random;

import linkpred.trust.bean.LinkFeatureId;
import util.ConnectionUtil;
import weka.bean.ModelResults;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.bayes.BayesNet;
import weka.classifiers.bayes.NaiveBayes;
import weka.classifiers.lazy.IBk;
import weka.classifiers.meta.AdaBoostM1;
import weka.classifiers.rules.JRip;
import weka.classifiers.trees.J48;
import weka.core.Instances;

/**
 * create single dataset based on features and runs classifiers on the generated
 * dataset
 * 
 * @author Zoheb Borbora
 */
public class LPExperiment {

	/**
	 * <pre>
	 * 	Node: 											1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16
	 *  TopologicalX (except closeness, eigenvector): 	17 18 21 22 23 24 25 26 27 28 29 30
	 * 
	 * 	NodeX1: 
	 * 	(except sum_char_sl_mins, diff_char_sl_mins):	1 2 3 4 5 7 9 10 11 12 13 14 15 16
	 *  TopologicalX1 
	 *    (except betwenness, closeness, eigenvector): 	17 21 22 23 24 25 26 27 28 29 30
	 * 
	 * 	NodeX2: 
	 * 	(except sum_char_sl_mins, diff_char_sl_mins):	1 4 5 6 7 8 12 13
	 *  TopologicalX2 
	 *    (except betwenness, closeness, eigenvector): 	17 21 22 23 24 25 26 27 28 29 30
	 * 
	 *  Cross network (EQ2): 							31 32 34
	 *  Cross network (CR3): 							62 32 63
	 * 
	 *  Cross network (EQ2): 							31 32 33 34 35
	 *  Cross network (CR3): 							31 32 33 34 35
	 *  Cross network (SB): 							60 61
	 *  Topological: 									17 18 19 20 21 22 23 24 25 26 27 28 29 30
	 *  Topological Weighted:  
	 *  
	 *  Node + TopologicalX + eq2-cross-network(f1)
	 *  	= 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35
	 *  TopologicalX + sb-cross-network(f2)
	 *  	= 17 18 21 22 23 24 25 26 27 28 29 30 60 61
	 * 
	 *  NodeX2 + TopologicalX2 + eq2-cross-network(f3)
	 *  	= 1 4 5 6 7 8 12 13 17 21 22 23 24 25 26 27 28 29 30 31 32 34
	 *  NodeX2 + TopologicalX2 + cr3-cross-network(f4)
	 *  	= 1 4 5 6 7 8 12 13 17 21 22 23 24 25 26 27 28 29 30 62 32 63
	 * </pre>
	 */

	private int[] NODE_FEATURES = new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
			11, 12, 13, 14, 15, 16 };

	private int[] TOPOLOGICAL_X_FEATURES = new int[] { 17, 18, 21, 22, 23, 24,
			25, 26, 27, 28, 29, 30 };

	private int[] EQ2_X_NETWORK_FEATURES = new int[] { 31, 32, 33, 34, 35 };

	private int[] SB_X_NETWORK_FEATURES = new int[] { 60, 61 };

	private int[] TOPOLOGICAL_FEATURES = new int[] { 17, 18, 19, 20, 21, 22,
			23, 24, 25, 26, 27, 28, 29, 30 };

	private int[] NETWORK_WEIGHTED_FEATURES = new int[] { 36, 37, 38, 39, 40,
			41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57,
			58, 59 };

	private int[] NODE_X2_FEATURES = new int[] { 1, 4, 5, 6, 7, 8, 12, 13 };

	private int[] TOPOLOGICAL_X2_FEATURES = new int[] { 17, 21, 22, 23, 24, 25,
			26, 27, 28, 29, 30 };

	private int[] EQ2_X2_NETWORK_FEATURES = new int[] { 31, 32, 33, 34 };

	private int[] CR3_X2_NETWORK_FEATURES = new int[] { 62, 32, 63 };

	/**
	 * Start: Configure this section
	 */
	private static String REL_NAME = "ep_neg_neg_f2";

	private static String REL_DESCRIPTION = "Epinion dataset - training: NEG, test: NEG";

	public void initFeatures() {
		appendFeatures(NODE_FEATURES);
		appendFeatures(TOPOLOGICAL_X_FEATURES);
		appendFeatures(EQ2_X2_NETWORK_FEATURES);
		// appendFeatures(CR3_X2_NETWORK_FEATURES);
		// appendFeatures(NETWORK_WEIGHTED_FEATURES);
	}

	private static String TABLE_NAME = "lp_edge_dataset";
	/**
	 * End: Configure this section
	 */

	private List<LinkFeatureId> FEATURES = new ArrayList<LinkFeatureId>();

	private static final String DATASET_FILE_PATH_PREFIX = "C:\\zborbor\\work\\trust\\datasets\\";

	private static final String RESULTS_FILE_NAME = "C:\\zborbor\\work\\trust\\datasets\\meta_results.csv";

	private int NUM_POSITIVE;

	private int NUM_NEGATIVE;

	private static String[] RELNAMES = new String[] { "eq2_GM_f1_c_3_21_1",
			"eq2_HH_f1_c_3_9_1", "eq2_HM_f1_c_3_14_1", "eq2_MH_f1_c_2_27_1",
			"eq2_MM_f1_c_3_15_1", "eq2_TH_f1_c_3_13_1", "eq2_TM_f1_c_3_20_1" };

	public static void main(String[] args) {
		// new LPExperiment().run();
		new LPExperiment().runAll();
	}

	public void runAll() {
		initFeatures();
		for (String relName : RELNAMES) {
			System.out.println("********** " + relName
					+ "Running classifiers..");
			runClassifiers(relName);
		}
		System.out.println("Finished!!");
	}

	public void run() {
		// initFeatures();
		// String relName = getUniqueRelName();
		// System.out.println("********** Creating dataset..");
		// generateDataset(relName);
		System.out.println("********** Running classifiers..");
		String relName = "eq2_GH_f1_c_3_10_1";
		runClassifiers(relName);
		System.out.println("Finished!!");
	}

	private void appendFeatures(int[] featureIds) {
		for (Integer featureId : featureIds) {
			FEATURES.add(LinkFeatureId.getTypeById(featureId));
		}
	}

	private void generateDataset(String relName) {

		Connection conn_src = null;
		BufferedWriter bufferedWriter = null;
		try {
			System.out.println("Genrating arff file " + relName);
			bufferedWriter = new BufferedWriter(new FileWriter(
					DATASET_FILE_PATH_PREFIX + relName + ".arff"));
			conn_src = getSourceConn();
			calculateDistribution(conn_src);
			writeHeader(bufferedWriter, relName);
			bufferedWriter.write("@DATA");
			bufferedWriter.newLine();
			writeData(bufferedWriter, conn_src);
			System.out.println("File generated: " + relName);
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
	}

	private void writeHeader(BufferedWriter bufferedWriter, String fileName)
			throws IOException {

		bufferedWriter.write("% Time generated: " + new Date());
		bufferedWriter.newLine();
		bufferedWriter.write("% Description: " + REL_DESCRIPTION);
		bufferedWriter.newLine();
		bufferedWriter.write("% Ratio of Positive:Negative " + NUM_POSITIVE
				+ ":" + +NUM_NEGATIVE);
		bufferedWriter.newLine();
		bufferedWriter.newLine();
		bufferedWriter.write("@RELATION " + fileName);
		bufferedWriter.newLine();
		bufferedWriter.write("@ATTRIBUTE player1_char_id NUMERIC");
		bufferedWriter.newLine();
		bufferedWriter.write("@ATTRIBUTE player2_char_id NUMERIC");
		bufferedWriter.newLine();
		for (LinkFeatureId feature : FEATURES) {
			bufferedWriter
					.write("@ATTRIBUTE " + feature.getName() + " NUMERIC");
			bufferedWriter.newLine();
		}
		bufferedWriter.write("@ATTRIBUTE form_link {Y,N}");
		bufferedWriter.newLine();
		bufferedWriter.newLine();
	}

	private void calculateDistribution(Connection conn) throws SQLException {

		String QUERY = "SELECT form_link,COUNT(*) AS cnt FROM " + TABLE_NAME
				+ " group by form_link";
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(QUERY);
		if (rs != null) {
			while (rs.next()) {
				boolean isChurner = rs.getString("form_link").equals("Y");
				if (isChurner) {
					NUM_POSITIVE = rs.getInt("cnt");
				} else {
					NUM_NEGATIVE = rs.getInt("cnt");
				}
			}
		}
	}

	private void writeData(BufferedWriter bufferedWriter, Connection conn)
			throws SQLException {

		Statement stmt = conn.createStatement();
		System.out.println("** Start: Fetch data");
		String query = "SELECT * FROM " + TABLE_NAME + " ORDER BY "
				+ TABLE_NAME + "_id";
		System.out.println("Query: " + query);
		ResultSet rs = stmt.executeQuery(query);
		System.out.println("** End: Fetch data");
		if (rs != null) {
			while (rs.next()) {
				writeLine(bufferedWriter, rs);
			}
		}
	}

	private String writeLine(BufferedWriter bufferedWriter, ResultSet rs) {

		String entry = null;
		try {
			entry = "";
			entry += rs.getLong("player1_char_id") + ",";
			entry += rs.getLong("player2_char_id") + ",";
			for (LinkFeatureId feature : FEATURES) {
				entry += handleNull(rs.getObject(feature.getName()), feature)
						+ ",";
			}
			entry += rs.getString("form_link");
			bufferedWriter.write(entry);
			bufferedWriter.newLine();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return entry;
	}

	private String handleNull(Object value, LinkFeatureId feature) {

		DecimalFormat twoDForm = new DecimalFormat("#.#####");
		String ret = null;
		if (value == null) {
			ret = "?";
		} else {
			if (value instanceof Double) {
				ret = twoDForm.format((Double) value);
			} else if (value instanceof Integer) {
				ret = String.valueOf((Integer) value);
			} else if (value instanceof Float) {
				ret = twoDForm.format((Float) value);
			}
		}

		return ret;
	}

	private String getUniqueRelName() {

		String fileName = REL_NAME;
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(System.currentTimeMillis());
		fileName += "_" + (cal.get(Calendar.MONTH) + 1) + "_"
				+ cal.get(Calendar.DAY_OF_MONTH);
		File dir = new File(DATASET_FILE_PATH_PREFIX);
		int cnt = 0;
		if (dir.isDirectory()) {
			for (String file : dir.list()) {
				if (file.endsWith(".arff")
						&& file.substring(0, file.lastIndexOf("_")).equals(
								fileName))
					cnt++;
			}
		}
		fileName += "_" + (cnt + 1);
		System.out.println(fileName);
		return fileName;
	}

	private void runClassifiers(String relName) {

		BufferedReader reader = null;
		BufferedWriter writer = null;
		try {

			writer = new BufferedWriter(new FileWriter(RESULTS_FILE_NAME, true));
			reader = new BufferedReader(new FileReader(DATASET_FILE_PATH_PREFIX
					+ relName + ".arff"));

			Instances data = new Instances(reader);
			// remove player ids
			data.deleteAttributeAt(1);
			data.deleteAttributeAt(0);
			// TODO: only for EQ2
			// int idx = data.attribute("link_in_pvp").index();
			// System.out.println("Removed:" + feature.getName());
			// data.deleteAttributeAt(idx);
			data.setClassIndex(data.numAttributes() - 1);

			// appendResultsToFile(new ModelResults(), writer, relName);
			runClassifier(writer, new J48(), data, relName);
			runClassifier(writer, new JRip(), data, relName);
			runClassifier(writer, new NaiveBayes(), data, relName);
			runClassifier(writer, new BayesNet(), data, relName);
			runClassifier(writer, new AdaBoostM1(), data, relName);
			runClassifier(writer, new IBk(3), data, relName);

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				writer.flush();
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private ModelResults runClassifier(BufferedWriter writer,
			Classifier classifer, Instances data, String relName) {

		ModelResults results = null;
		try {

			String classifierName = classifer.getClass().getSimpleName();
			System.out.println("*** Runnning " + classifierName + ".. ");

			Evaluation eval = new Evaluation(data);
			eval.crossValidateModel(classifer, data, 10, new Random(1));
			double cfm[][] = eval.confusionMatrix();

			results = new ModelResults();
			results.setTruePositive((int) cfm[0][0]);
			results.setFalseNegative((int) cfm[0][1]);
			results.setFalsePositive((int) cfm[1][0]);
			results.setTrueNegative((int) cfm[1][1]);
			results.setPrecisionY((float) eval.precision(0));
			results.setRecallY((float) eval.recall(0));
			results.setfMeasureY((float) eval.fMeasure(0));
			results.setPrecisionN((float) eval.precision(1));
			results.setRecallN((float) eval.recall(1));
			results.setfMeasureN((float) eval.fMeasure(1));
			results.setClassifierName(classifierName);
			results.setNumY(NUM_POSITIVE);
			results.setNumN(NUM_NEGATIVE);
			printResults(results);
			appendResultsToFile(results, writer, relName);

		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return results;
	}

	private void appendResultsToFile(ModelResults results,
			BufferedWriter bufferedWriter, String relName) throws IOException {

		try {
			bufferedWriter = new BufferedWriter(new FileWriter(
					RESULTS_FILE_NAME, true));

			System.out.println("writing to file");
			bufferedWriter.write(relName + "\t" + results.getClassifierName()
					+ "\t" + results.getNumY() + "\t" + results.getNumN()
					+ "\t" + results.getPrecisionY() + "\t"
					+ results.getRecallY() + "\t" + results.getfMeasureY());
			bufferedWriter.newLine();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
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

	private Connection getSourceConn() {
		return ConnectionUtil.getGUILETrustConnection(false);
	}
}
