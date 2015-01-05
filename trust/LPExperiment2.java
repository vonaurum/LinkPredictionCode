/*
 * linkpred.trust.LPExperiment1.java
 *
 * Created on May 24, 2011
 */
package linkpred.trust;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import linkpred.trust.bean.LinkFeatureId;
import weka.attributeSelection.InfoGainAttributeEval;
import weka.bean.ModelResults;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.rules.JRip;
import weka.core.Attribute;
import weka.core.Instances;

/**
 * run model on dataset with top k features
 * 
 * @author zborbor
 */
public class LPExperiment2 {

	private static final String INPUT_PATH_PREFIX = "C:\\zborbor\\Dropbox\\private\\research - Trust\\datasets\\";

	private static final String OUTPUT_PATH_PREFIX = "C:\\zborbor\\Dropbox\\private\\research - Trust\\datasets\\results\\ablation_";

	private static NumberFormat NF = new DecimalFormat("##.######");

	private static int[] EQ2_FEATURES = new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9,
			10, 11, 12, 13, 14, 15, 16, 17, 18, 21, 22, 23, 24, 25, 26, 27, 28,
			29, 30, 31, 32, 33, 34 };

	private static int[] SB_FEATURES = new int[] { 17, 18, 21, 22, 23, 24, 25,
			26, 27, 28, 29, 30, 60, 61 };

	private static int[] EQ2_FEATURES1 = new int[] { 1, 4, 5, 6, 7, 8, 12, 13,
			17, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 34 };

	private static int[] CR3_FEATURES1 = new int[] { 1, 4, 5, 6, 7, 8, 12, 13,
			17, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 62, 32, 63 };

	/*
	 * list of features sorted in increasing order of average info-gain ranking
	 * across all EQ2 datasets
	 */
	private static int[] EQ2_AVG_SORTED = new int[] { 2, 1, 3, 11, 27, 28, 4,
			5, 9, 10, 7, 31, 32, 33, 16, 15, 8, 34, 14, 17, 22, 18, 24, 26, 30,
			29, 25, 21, 13, 23, 6, 12 };

	private static int[] EQ2_NEG_A_AVG_SORTED = new int[] { 2, 4, 5, 7, 10, 15,
			17, 22, 27, 28, 32, 34, 12, 33, 16, 21, 8, 13, 1, 3, 9, 18, 24, 29,
			31, 6, 30, 14, 25, 26, 11, 23 };

	private static int[] SB_AVG_SORTED = new int[] { 27, 28, 61, 60, 30, 29,
			25, 26, 24, 18, 23, 17, 22, 21 };

	private static int[] EQ2_AVG_SORTED1 = new int[] { 1, 27, 28, 4, 5, 32, 31,
			7, 8, 17, 22, 34, 24, 26, 30, 25, 29, 21, 23, 6, 13, 12 };

	private static int[] CR3_AVG_SORTED1 = new int[] { 1, 4, 27, 28, 5, 7, 62,
			32, 13, 12, 63, 30, 29, 26, 25, 17, 22, 24, 8, 23, 21, 6 };

	private static Set<String> FEATURE_NAMES = new HashSet<String>();

	private final Comparator<Double> reverseComparator = new Comparator<Double>() {
		public int compare(Double left, Double right) {
			return -1 * left.compareTo(right);
		}
	};

	private SortedMap<Double, List<LinkFeatureId>> SORTED_INFO_GAIN_MAP = new TreeMap<Double, List<LinkFeatureId>>();

	private static String[] RELNAMES = new String[] { "eq2_HH_f1_a_2_20_1" };

	private static String[] RELNAMES1 = new String[] { "eq2_GG_f1_a_4_2_1",
			"eq2_GH_f1_a_3_9_1", "eq2_GM_f1_a_3_20_1", "eq2_GT_f1_a_3_27_1",
			"eq2_HG_f1_a_3_29_1", "eq2_HH_f1_a_2_20_1", "eq2_HM_f1_a_3_14_1",
			"eq2_HT_f1_a_3_22_1", "eq2_MG_f1_a_3_29_1", "eq2_MH_f1_a_2_26_1",
			"eq2_MM_f1_a_3_14_1", "eq2_MT_f1_a_3_24_1", "eq2_TG_f1_a_3_30_1",
			"eq2_TH_f1_a_3_13_1", "eq2_TM_f1_a_3_20_1", "eq2_TT_f1_a_3_25_1" };

	/*
	 * Start: Configure section
	 */

	private static final int[] K_ARRAY = new int[] { 1, 2, 3, 4, 5, 7, 10, 15,
			20, 25, 32 };

	private static final String RELNAME = "guk_housing_housing_f3_feb_jun_aug_3_11_1";

	private static int[] FEATURES = EQ2_FEATURES;

	private static int[] AVG_FEATURES = EQ2_NEG_A_AVG_SORTED;

	/*
	 * End: Configure section
	 */

	public static void main(String[] args) {
		setFeaturesSet();
		for (String relName : RELNAMES) {
			System.out.println("********** Running classifiers..");
			new LPExperiment2().run(relName);
		}
		System.out.println("****** Finished!");
	}

	public void run(String relName) {

		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(OUTPUT_PATH_PREFIX
					+ relName + ".csv"));
			writer.write("Dataset: " + relName);
			writer.newLine();
			writer.newLine();

			runSpecific(writer, relName);
			runAverage(writer, relName);

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
		System.out.println("****** Finished!");
	}

	public void runSpecific(BufferedWriter writer, String relName)
			throws IOException {
		System.out
				.println("****** Running for dataset specific top attributes.. ");
		System.out
				.println("*** Calculating info-gain ranking for all attributes.. ");
		infoGainRanking(relName);
		// print(SORTED_INFO_GAIN_MAP);
		writer.write("Sorted features (descending in info gain).. ");
		writer.newLine();
		writeDesc(writer);
		writer.newLine();

		writer.write("Dataset-specific attributes");
		writer.newLine();
		for (int i = 0; i < K_ARRAY.length; i++) {
			int k = K_ARRAY[i];
			System.out.println("*** Running model for top " + k
					+ " attributes.. ");
			runModelOnSpecifc(writer, relName, k);
		}
	}

	public void runAverage(BufferedWriter writer, String relName)
			throws IOException {
		System.out
				.println("****** Running for dataset average top attributes.. ");

		writer.newLine();
		writer.write("Average attributes");
		writer.newLine();
		for (int i = 0; i < K_ARRAY.length; i++) {
			int k = K_ARRAY[i];
			System.out.println("*** Running model for top " + k
					+ " attributes.. ");
			runModelOnAverage(writer, relName, k);
		}
	}

	private static void setFeaturesSet() {
		for (int i = 0; i < FEATURES.length; i++) {
			LinkFeatureId linkFeatureId = LinkFeatureId
					.getTypeById(FEATURES[i]);
			FEATURE_NAMES.add(linkFeatureId.getName());
		}
		FEATURE_NAMES.add("form_link");
	}

	private void infoGainRanking(String relName) {

		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(INPUT_PATH_PREFIX
					+ relName + ".arff"));
			Instances data = new Instances(reader);
			removeExtraAttributes(data);
			data.setClassIndex(data.numAttributes() - 1);

			InfoGainAttributeEval attributeEval = new InfoGainAttributeEval();
			attributeEval.buildEvaluator(data);
			for (int i = 0; i < FEATURES.length; i++) {
				int featureId = FEATURES[i];
				Double key = new Double(attributeEval.evaluateAttribute(i));
				List<LinkFeatureId> value = null;
				if (!SORTED_INFO_GAIN_MAP.containsKey(key)) {
					value = new ArrayList<LinkFeatureId>();
					SORTED_INFO_GAIN_MAP.put(key, value);
				} else {
					value = SORTED_INFO_GAIN_MAP.get(key);
				}
				value.add(LinkFeatureId.getTypeById(featureId));
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private void runModelOnSpecifc(BufferedWriter writer, String relName, int k) {

		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(INPUT_PATH_PREFIX
					+ relName + ".arff"));
			Instances data = new Instances(reader);
			removeExtraAttributes(data);
			data.setClassIndex(data.numAttributes() - 1);

			System.out.println(" Removing " + (FEATURE_NAMES.size() - 1 - k)
					+ " attributes.. ");
			int kPrime = FEATURES.length - k;
			int removedCount = 0;
			for (Map.Entry<Double, List<LinkFeatureId>> mapEntry : SORTED_INFO_GAIN_MAP
					.entrySet()) {
				for (LinkFeatureId feature : mapEntry.getValue()) {
					if (removedCount < kPrime) {
						Attribute attrib = data.attribute(feature.getName());
						int idx = attrib.index();
						// System.out.println("Removed:" + feature.getName());
						data.deleteAttributeAt(idx);
						removedCount++;
					}
				}
			}
			System.out.println("Num of attributes: "
					+ (data.numAttributes() - 1));
			// runClassifier(writer, new J48(), data, k);
			runClassifier(writer, new JRip(), data, k);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private void runModelOnAverage(BufferedWriter writer, String relName, int k) {

		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(INPUT_PATH_PREFIX
					+ relName + ".arff"));
			Instances data = new Instances(reader);
			removeExtraAttributes(data);
			data.setClassIndex(data.numAttributes() - 1);

			System.out.println(" Removing " + (FEATURE_NAMES.size() - 1 - k)
					+ " attributes.. ");
			int kPrime = FEATURES.length - k;
			int removedCount = 0;
			for (int i = 0; i < AVG_FEATURES.length; i++) {

				LinkFeatureId feature = LinkFeatureId
						.getTypeById(AVG_FEATURES[i]);
				if (removedCount < kPrime) {
					Attribute attrib = data.attribute(feature.getName());
					int idx = attrib.index();
					// System.out.println("Removed:" + feature.getName());
					data.deleteAttributeAt(idx);
					removedCount++;
				}
			}

			System.out.println("Num of attributes: "
					+ (data.numAttributes() - 1));
			// runClassifier(writer, new J48(), data, k);
			runClassifier(writer, new JRip(), data, k);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/*
	 * remove attributes not in the FEATURE_NAMES
	 */
	private void removeExtraAttributes(Instances data) {

		System.out.println(" Removing extra attributes.. ");
		List<String> exclusionList = new ArrayList<String>();
		for (int i = 0; i < data.numAttributes(); i++) {
			Attribute attrib = data.attribute(i);
			if (!FEATURE_NAMES.contains(attrib.name())) {
				exclusionList.add(attrib.name());
			}
		}

		for (String attribName : exclusionList) {
			Attribute attrib = data.attribute(attribName);
			data.deleteAttributeAt(attrib.index());
			// System.out.println("Removed " + attribName);
		}
	}

	private ModelResults runClassifier(BufferedWriter writer,
			Classifier classifer, Instances data, int k) {

		ModelResults results = null;
		try {
			String classifierName = classifer.getClass().getSimpleName();
			System.out.println(" Runnning " + classifierName + ".. ");

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
			printResults(results, k);
			appendResultsToFile(results, writer, k);

		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return results;
	}

	private void appendResultsToFile(ModelResults results,
			BufferedWriter bufferedWriter, int k) throws IOException {

		bufferedWriter.write(results.getClassifierName() + "\t" + k + "\t"
				+ NF.format(results.getPrecisionY()) + "\t"
				+ NF.format(results.getRecallY()) + "\t"
				+ NF.format(results.getfMeasureY()));
		bufferedWriter.newLine();
	}

	private void printResults(ModelResults results, int k) {

		System.out.print("k = " + k + " Confusion matrix: ");
		System.out.print("TP = " + results.getTruePositive() + " FN = "
				+ results.getFalseNegative() + " FP = "
				+ results.getFalsePositive() + " TN= "
				+ results.getTrueNegative());
		System.out
				.print("\tPrecision " + results.getPrecisionY() + " Recall "
						+ results.getRecallY() + " F-measure "
						+ results.getfMeasureY());
		System.out.println();
	}

	private void writeDesc(BufferedWriter writer) throws IOException {

		SortedMap<Double, List<LinkFeatureId>> descMap = new TreeMap<Double, List<LinkFeatureId>>(
				reverseComparator);
		for (Map.Entry<Double, List<LinkFeatureId>> mapEntry : SORTED_INFO_GAIN_MAP
				.entrySet()) {
			descMap.put(mapEntry.getKey(), mapEntry.getValue());
		}
		int count = 0;
		for (Map.Entry<Double, List<LinkFeatureId>> mapEntry : descMap
				.entrySet()) {
			for (LinkFeatureId feature : mapEntry.getValue()) {
				writer.write(++count + ": " + feature.getName() + "\t"
						+ mapEntry.getKey());
				writer.newLine();
			}
		}
	}

	private void print(SortedMap<Double, List<LinkFeatureId>> sortedMap) {

		for (Map.Entry<Double, List<LinkFeatureId>> mapEntry : sortedMap
				.entrySet()) {
			System.out.print(mapEntry.getKey() + ": ");
			for (LinkFeatureId feature : mapEntry.getValue()) {
				System.out.println("\t" + feature.getName());
			}
		}
	}
}