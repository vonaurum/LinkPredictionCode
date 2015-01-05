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
import weka.attributeSelection.AttributeSelection;
import weka.attributeSelection.BestFirst;
import weka.attributeSelection.CfsSubsetEval;
import weka.attributeSelection.InfoGainAttributeEval;
import weka.bean.ModelResults;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.rules.JRip;
import weka.classifiers.trees.J48;
import weka.core.Attribute;
import weka.core.Instances;

/**
 * run model on dataset with top k CFS features
 * 
 * @author zborbor
 */
public class LPExperiment3 {

	private static final String INPUT_PATH_PREFIX = "C:\\zborbor\\work\\trust\\datasets\\";

	private static final String OUTPUT_PATH_PREFIX = "C:\\zborbor\\work\\trust\\results\\cfm_ablation_";

	private static NumberFormat NF = new DecimalFormat("##.######");

	private static int[] EQ2_FEATURES = new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9,
			10, 11, 12, 13, 14, 15, 16, 17, 18, 21, 22, 23, 24, 25, 26, 27, 28,
			29, 30, 31, 32, 33, 34 };

	private static int[] SB_FEATURES = new int[] { 17, 18, 21, 22, 23, 24, 25,
			26, 27, 28, 29, 30, 60, 61 };

	private static int[] EPINION_FEATURES = new int[] { 17, 18, 21, 22, 23, 24,
			25, 26, 27, 28, 29, 30, 60, 61 };

	/*
	 * list of features sorted in increasing order of average CFS ranking across
	 * all EQ2 datasets
	 */
	private static int[] EQ2_CFS_AVG_SORTED = new int[] { 2, 8, 16, 17, 22, 28,
			10, 1, 24, 3, 18, 27, 15, 5, 25, 31, 32, 33, 7, 21, 26, 29, 9, 13,
			11, 30, 23, 34, 4, 12, 6, 14 };

	private static int[] EQ2_NEG_A_CFS_AVG_SORTED = new int[] { 2, 4, 5, 7, 10,
			15, 17, 22, 27, 28, 32, 34, 12, 33, 16, 21, 8, 13, 1, 3, 9, 18, 24,
			29, 31, 6, 30, 14, 25, 26, 11, 23 };

	private static int[] SB_CFS_AVG_SORTED = new int[] { 17, 22, 27, 28, 30,
			25, 29, 61, 60, 24, 18, 23, 26, 21 };

	private static int[] EPINION_CFS_AVG_SORTED = new int[] { 17, 22, 27, 28,
			30, 61, 60, 29, 24, 18, 26, 25, 23, 21 };

	private static Set<String> FEATURE_NAMES = new HashSet<String>();

	private final Comparator<Double> reverseComparator = new Comparator<Double>() {
		public int compare(Double left, Double right) {
			return -1 * left.compareTo(right);
		}
	};

	private SortedMap<Double, List<LinkFeatureId>> SORTED_INFO_GAIN_MAP = new TreeMap<Double, List<LinkFeatureId>>();

	private List<LinkFeatureId> SORTED_INFO_FEATURES = new ArrayList<LinkFeatureId>();

	private List<LinkFeatureId> SORTED_CFS_FEATURES = new ArrayList<LinkFeatureId>();

	private List<LinkFeatureId> SORTED_CFS_ALL_FEATURES = new ArrayList<LinkFeatureId>();

	private static String[] RELNAMES1 = new String[] {
			// "guk_housing_housing_f1_feb_jun_aug_5_22_1",
			// "guk_mentoring_housing_f1_feb_jun_aug_5_23_1",
			// "guk_trade_housing_f1_feb_jun_aug_5_24_1",
			"guk_group_housing_f1_feb_jun_aug_5_25_1",
			"guk_housing_mentoring_f1_feb_jun_aug_5_23_1",
			"guk_mentoring_mentoring_f1_feb_jun_aug_5_22_1",
			"guk_trade_mentoring_f1_feb_jun_aug_5_24_1",
			"guk_group_mentoring_f1_feb_jun_aug_5_27_1",
			"guk_housing_trade_f1_feb_jun_aug_5_23_1",
			"guk_mentoring_trade_f1_feb_jun_aug_5_23_1",
			"guk_trade_trade_f1_feb_jun_aug_5_22_1",
			"guk_group_trade_f1_feb_jun_aug_5_28_1",

			"guk_housing_group_f1_feb_jun_aug_5_23_1",
			"guk_mentoring_group_f1_feb_jun_aug_5_24_1",
			"guk_trade_group_f1_feb_jun_aug_5_25_1",
			"guk_group_group_f1_feb_jun_aug_5_23_1" };

	private static String[] RELNAMES2 = new String[] {
			"ibm_neg_neg_f2_jan_aug_dec_6_1_1",
			"ibm_neg_pos_f2_jan_aug_dec_6_2_1",
			"ibm_pos_neg_f2_jan_aug_dec_6_2_1",
			"ibm_pos_pos_f2_jan_aug_dec_6_1_1" };

	private static String[] RELNAMES3 = new String[] { "ep_pos_pos_f2_6_18_1",
			"ep_neg_pos_f2_6_19_1", "ep_pos_neg_f2_6_19_1",
			"ep_neg_neg_f2_6_20_1" };

	private static String[] RELNAMES = new String[] { // "eq2_GG_f1_a_4_2_1",
			// "eq2_GH_f1_a_3_9_1", "eq2_GM_f1_a_3_20_1", "eq2_HH_f1_a_2_20_1"
			//"eq2_GT_f1_a_3_27_1", 
		"eq2_HG_f1_a_3_29_1", "eq2_HM_f1_a_3_14_1",
			"eq2_HT_f1_a_3_22_1", "eq2_MG_f1_a_3_29_1", "eq2_MH_f1_a_2_26_1",
			"eq2_MM_f1_a_3_14_1", "eq2_MT_f1_a_3_24_1", "eq2_TG_f1_a_3_30_1",
			"eq2_TH_f1_a_3_13_1", "eq2_TM_f1_a_3_20_1", "eq2_TT_f1_a_3_25_1" };

	/*
	 * Start: Configure section
	 */

	private static final int[] K_ARRAY = new int[] { 1, 2, 3, 4, 5, 7, 10, 15,
			20, 25, 32 };

	private static final String RELNAME = "eq2_HH_f1_a_2_20_1";

	private static int[] FEATURES = EQ2_FEATURES;

	private static int[] AVG_FEATURES = EQ2_NEG_A_CFS_AVG_SORTED;

	/*
	 * End: Configure section
	 */

	public static void main(String[] args) {
		setFeaturesSet();
		for (String relName : RELNAMES) {
			System.out.println("********** Running classifiers for " + relName);
			new LPExperiment3().run(relName);
		}
		// new LPExperiment3().run(RELNAME);
		System.out.println("****** Finished all!");
	}

	public void run(String relName) {

		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(OUTPUT_PATH_PREFIX
					+ relName + ".csv"));
			writer.write("Dataset: " + relName);
			writer.newLine();
			writer.newLine();

			System.out.println("****** Running for " + relName);
			float maxFmeasure = maxFmeasure(relName);
			System.out.println("****** maxFmeasure for " + relName + ": "
					+ maxFmeasure);
			runSpecific(writer, relName, maxFmeasure);
			runAverage(writer, relName, maxFmeasure);

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
		System.out.println("****** Finished run!");
	}

	private float maxFmeasure(String relName) {

		float maxFMeasure = 1f;
		BufferedReader bufferedReader = null;
		try {
			bufferedReader = new BufferedReader(new FileReader(
					INPUT_PATH_PREFIX + "meta_results.csv"));

			Classifier classifer = new JRip();
			String classifierName = classifer.getClass().getSimpleName();
			String line = bufferedReader.readLine();
			while (line != null) {
				String[] s = line.split("\t");
				if (relName.equals(s[0]) && classifierName.equals(s[1])) {
					maxFMeasure = Float.valueOf(s[6]);
					break;
				}
				line = bufferedReader.readLine();
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				bufferedReader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return maxFMeasure;
	}

	public void runSpecific(BufferedWriter writer, String relName,
			float maxFmeasure) throws IOException {
		System.out
				.println("****** Running for dataset specific top attributes.. ");
		System.out
				.println("***** Calculating info-gain ranking for all attributes.. ");
		infoGainRanking(relName);
		// print(SORTED_INFO_GAIN_MAP);
		System.out.println("*** Features ranked by info-gain (increasing)");
		print(SORTED_INFO_FEATURES);
		cfsRanking(relName);
		System.out
				.println("*** CFS selected features - ranked by info-gain (increasing)");
		print(SORTED_CFS_FEATURES);
		/*
		 * System.out .println(
		 * "*** List of CFS non-selected and selected features (ranked by info-gain (increasing))"
		 * ); print(SORTED_CFS_ALL_FEATURES);
		 */
		writer.write("Sorted CFS features (descending in info gain).. ");
		writer.newLine();
		writeDesc(writer);
		writer.newLine();

		writer.write("Dataset-specific attributes");
		writer.newLine();
		float fMeasure = 0;
		for (int i = 0; i < K_ARRAY.length && fMeasure < maxFmeasure; i++) {
			int k = K_ARRAY[i];
			System.out.println("*** Running model for top " + k
					+ " CFS attributes.. ");
			fMeasure = runModelOnSpecifc(writer, relName, k);
		}
	}

	public void runAverage(BufferedWriter writer, String relName,
			float maxFmeasure) throws IOException {
		System.out
				.println("****** Running for dataset average top attributes.. ");

		writer.newLine();
		writer.write("Average attributes");
		writer.newLine();
		float fMeasure = 0;
		for (int i = 0; i < K_ARRAY.length && fMeasure < maxFmeasure; i++) {
			int k = K_ARRAY[i];
			System.out.println("*** Running model for top " + k
					+ " CFS average attributes.. ");
			fMeasure = runModelOnAverage(writer, relName, k);
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

			for (Map.Entry<Double, List<LinkFeatureId>> mapEntry : SORTED_INFO_GAIN_MAP
					.entrySet()) {
				for (LinkFeatureId feature : mapEntry.getValue()) {
					SORTED_INFO_FEATURES.add(feature);
				}
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

	private void cfsRanking(String relName) {

		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(INPUT_PATH_PREFIX
					+ relName + ".arff"));
			Instances data = new Instances(reader);
			removeExtraAttributes(data);
			data.setClassIndex(data.numAttributes() - 1);

			AttributeSelection attsel = new AttributeSelection();
			CfsSubsetEval eval = new CfsSubsetEval();
			BestFirst search = new BestFirst();
			attsel.setEvaluator(eval);
			attsel.setSearch(search);
			attsel.SelectAttributes(data);
			// obtain the attribute indices that were selected
			int[] indices = attsel.selectedAttributes();
			for (LinkFeatureId feature : SORTED_INFO_FEATURES) {
				for (int i : indices) {
					String featureName = data.attribute(i).name();
					if (feature.getName().equals(featureName)) {

						SORTED_CFS_FEATURES.add(feature);
						break;
					}
				}
			}
			for (LinkFeatureId feature : SORTED_INFO_FEATURES) {
				if (!SORTED_CFS_FEATURES.contains(feature)) {
					SORTED_CFS_ALL_FEATURES.add(feature);
				}
			}
			SORTED_CFS_ALL_FEATURES.addAll(SORTED_CFS_FEATURES);

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

	private float runModelOnSpecifc(BufferedWriter writer, String relName, int k) {

		float fMeasure = 0f;
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
			for (LinkFeatureId feature : SORTED_CFS_ALL_FEATURES) {
				if (removedCount < kPrime) {
					Attribute attrib = data.attribute(feature.getName());
					int idx = attrib.index();
					// System.out.println("Removed:" + feature.getName());
					data.deleteAttributeAt(idx);
					removedCount++;
				}
			}
			System.out.println("Available num of attributes: "
					+ (data.numAttributes() - 1));
			// runClassifier(writer, new J48(), data, k);
			ModelResults modelResults = runClassifier(writer, new JRip(), data,
					k);
			fMeasure = modelResults.getfMeasureY();
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

		return fMeasure;
	}

	private float runModelOnAverage(BufferedWriter writer, String relName, int k) {

		float fMeasure = 0f;
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
			ModelResults modelResults = runClassifier(writer, new JRip(), data,
					k);
			fMeasure = modelResults.getfMeasureY();
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

		return fMeasure;
	}

	/*
	 * remove attributes not in the FEATURE_NAMES
	 */
	private void removeExtraAttributes(Instances data) {

		System.out.println("*** Removing extra attributes.. ");
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

		int count = 0;
		for (int i = SORTED_CFS_FEATURES.size() - 1; i >= 0; i--) {
			LinkFeatureId feature = SORTED_CFS_FEATURES.get(i);
			writer.write(++count + ": " + feature.getName());
			writer.newLine();
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

	private void print(List<LinkFeatureId> features) {
		for (LinkFeatureId feature : features) {
			System.out.println(feature.getName());
		}
	}
}