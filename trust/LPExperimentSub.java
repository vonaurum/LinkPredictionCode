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
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
import weka.core.Attribute;
import weka.core.Instance;
import weka.core.Instances;

/**
 * create subset of single dataset based on features and runs classifiers on the
 * generated dataset
 * 
 * @author Zoheb Borbora
 */
public class LPExperimentSub {

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

	private int[] EQ2_X2_NETWORK_FEATURES = new int[] { 31, 32, 34 };

	private int[] CR3_X2_NETWORK_FEATURES = new int[] { 62, 32, 63 };

	/**
	 * Start: Configure this section
	 */

	private static String INPUT_REL_NAME = "guk_mentoring_mentoring_f1_feb_jun_aug_5_22_1";

	private static String REL_DESCRIPTION = "EQII dataset (guk) - "
			+ "training period: FEB-JUN (mentoring), test period: : JUL-AUG (mentoring)";

	public void initFeatures() {
		appendFeatures(NODE_X2_FEATURES);
		appendFeatures(TOPOLOGICAL_X2_FEATURES);
		// appendFeatures(SB_X_NETWORK_FEATURES);
		appendFeatures(EQ2_X2_NETWORK_FEATURES);
		// appendFeatures(NETWORK_WEIGHTED_FEATURES);
	}

	/**
	 * End: Configure this section
	 */

	private List<LinkFeatureId> FEATURES = new ArrayList<LinkFeatureId>();

	private static final String DATASET_FILE_PATH_PREFIX = "C:\\zborbor\\work\\trust\\datasets\\";

	private static final String RESULTS_FILE_NAME = "C:\\zborbor\\work\\trust\\datasets\\meta_results.csv";

	private int NUM_POSITIVE;

	private int NUM_NEGATIVE;

	private static String[] RELNAMES = new String[] {};

	public static void main(String[] args) {
		new LPExperimentSub().run();
		// new LPExperiment().runAll();
		// new LPExperimentSub().getUniqueRelName();
	}

	public void runAll() {
		initFeatures();
		for (String relName : RELNAMES) {
			System.out.println("********** Running classifiers..");
			runClassifiers(relName);
		}
		System.out.println("Finished!!");
	}

	public void run() {
		initFeatures();
		String relName = getUniqueRelName();
		System.out.println("********** Creating dataset..");
		generateDataset(relName);
		System.out.println("********** Running classifiers..");
		// relName = "nagafen_housing_housing_f3_feb_jun_aug_6_14_1";
		// runClassifiers(relName);
		System.out.println("Finished!!");
	}

	private void appendFeatures(int[] featureIds) {
		for (Integer featureId : featureIds) {
			FEATURES.add(LinkFeatureId.getTypeById(featureId));
		}
	}

	private void generateDataset(String relName) {

		BufferedReader bufferedReader = null;
		BufferedWriter bufferedWriter = null;
		try {
			System.out.println("Genrating arff file " + relName);
			bufferedWriter = new BufferedWriter(new FileWriter(
					DATASET_FILE_PATH_PREFIX + relName + ".arff"));
			bufferedReader = new BufferedReader(new FileReader(
					DATASET_FILE_PATH_PREFIX + "eq2\\" + INPUT_REL_NAME
							+ ".arff"));
			Instances inputData = new Instances(bufferedReader);

			calculateDistribution();
			writeHeader(bufferedWriter, relName);
			bufferedWriter.write("@DATA");
			bufferedWriter.newLine();
			writeData(bufferedWriter, inputData);
			System.out.println("File generated: " + relName);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				bufferedWriter.flush();
				bufferedWriter.close();
				bufferedReader.close();
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

	private void calculateDistribution() throws IOException {

		BufferedReader bufferedReader = new BufferedReader(new FileReader(
				DATASET_FILE_PATH_PREFIX + "eq2\\" + INPUT_REL_NAME + ".arff"));

		String substr = "% Ratio of Positive:Negative ";
		String line = bufferedReader.readLine();
		while (line != null) {
			if (line.startsWith(substr)) {
				String dist = line.substring(substr.length(), line.length());
				// System.out.println(dist);
				String[] s = dist.split(":");
				NUM_POSITIVE = Integer.valueOf(s[0]);
				NUM_NEGATIVE = Integer.valueOf(s[1]);
				break;
			}
			line = bufferedReader.readLine();
		}
	}

	private void writeData(BufferedWriter bufferedWriter, Instances inputData) {

		Iterator<Instance> itr = inputData.iterator();
		Map<String, Attribute> attributeMap = new HashMap<String, Attribute>();
		Enumeration<Attribute> attEnum = inputData.enumerateAttributes();
		while (attEnum.hasMoreElements()) {
			Attribute attribute = attEnum.nextElement();
			attributeMap.put(attribute.name(), attribute);
		}
		while (itr.hasNext()) {
			writeLine(bufferedWriter, attributeMap, itr.next());
		}
	}

	private String writeLine(BufferedWriter bufferedWriter,
			Map<String, Attribute> attributeMap, Instance inputInstance) {

		String entry = null;
		try {
			entry = "";
			entry += inputInstance.value(attributeMap.get("player1_char_id"))
					+ ",";
			entry += inputInstance.value(attributeMap.get("player2_char_id"))
					+ ",";
			for (LinkFeatureId feature : FEATURES) {
				if (!inputInstance
						.isMissing(attributeMap.get(feature.getName()))) {
					entry += inputInstance.value(attributeMap.get(feature
							.getName())) + ",";
				} else {
					entry += "?,";

				}
			}
			entry += (inputInstance.value(attributeMap.get("form_link")) == 0 ? "Y"
					: "N");
			bufferedWriter.write(entry);
			bufferedWriter.newLine();
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

		String fileName = INPUT_REL_NAME.substring(0,
				INPUT_REL_NAME.indexOf("_aug") + "_aug".length()).replace("f1",
				"f3");
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
		System.out.println("Input: " + INPUT_REL_NAME + " Output: " + fileName);
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

		bufferedWriter.write(relName + "\t" + results.getClassifierName()
				+ "\t" + results.getNumY() + "\t" + results.getNumN() + "\t"
				+ results.getPrecisionY() + "\t" + results.getRecallY() + "\t"
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

	private Connection getSourceConn() {
		return ConnectionUtil.getGUILETrustConnection(false);
	}
}
