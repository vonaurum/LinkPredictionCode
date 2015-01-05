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

import linkpred.trust.bean.LinkFeatureId;

import weka.attributeSelection.InfoGainAttributeEval;
import weka.core.Instances;

/**
 * generate info-gain values for a dataset
 * 
 * @author zborbor
 */
public class LPExperiment1 {

	private static final String PATH_PREFIX = "C:\\zborbor\\work\\trust\\datasets\\";

	private static final String INPUT_FILE = "eq2_TM_f1_c_3_20_1.arff";

	private static final String OUTPUT_FILE = "info_gain_results.csv";

	private static int[] EQ2_FEATURES = new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9,
			10, 11, 12, 13, 14, 15, 16, 17, 18, 21, 22, 23, 24, 25, 26, 27, 28,
			29, 30, 31, 32, 33, 34 };

	private static int[] SB_FEATURES = new int[] { 17, 18, 21, 22, 23, 24, 25,
			26, 27, 28, 29, 30, 60, 61 };

	private static int[] EQ2_FEATURES1 = new int[] { 1, 4, 5, 6, 7, 8, 12, 13,
			17, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 34 };

	private static int[] EPINION_FEATURES = new int[] { 17, 18, 21, 22, 23, 24,
			25, 26, 27, 28, 29, 30, 60, 61 };

	private static int[] CR3_FEATURES1 = new int[] { 1, 4, 5, 6, 7, 8, 12, 13,
			17, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 62, 32, 63 };

	private static int[] FEATURES = EQ2_FEATURES;

	private static NumberFormat NF = new DecimalFormat("##.######");

	public static void main(String[] args) {
		infoGainRanking();
	}

	public static void infoGainRanking() {

		BufferedReader reader = null;
		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(PATH_PREFIX
					+ OUTPUT_FILE));
			writer.write(INPUT_FILE);
			writer.newLine();
			writer.newLine();
			reader = new BufferedReader(
					new FileReader(PATH_PREFIX + INPUT_FILE));
			Instances data = new Instances(reader);
			// remove char ids
			data.deleteAttributeAt(1);
			data.deleteAttributeAt(0);
			data.setClassIndex(data.numAttributes() - 1);

			InfoGainAttributeEval attributeEval = new InfoGainAttributeEval();
			attributeEval.buildEvaluator(data);
			for (int i = 0; i < FEATURES.length; i++) {
				int featureId = FEATURES[i];
				writer.write(LinkFeatureId.getTypeById(featureId).getName()
						+ "," + NF.format(attributeEval.evaluateAttribute(i)));
				System.out
						.println(NF.format(attributeEval.evaluateAttribute(i)));
				writer.newLine();
			}

			// System.out.println("Finished!");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				reader.close();
				writer.flush();
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
