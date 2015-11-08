package csc555_assignment6;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class MyJoinReducerPass1 extends Reducer<Text, Text, Text, Text> {
	private Text _malware = new Text();
	private Logger logger = Logger.getLogger(MyJoinReducerPass1.class);

	/**
	 * @param key:
	 *            example 192.0.0.1
	 * @param values:
	 *            example [i_TrojanHorse,i_SpyEye,w_,w_,i_Zbot]
	 */
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// List<Text> weblogValues = new ArrayList<Text>();
		// List<Text> infectedValues = new ArrayList<Text>();
		ArrayList<String> values1 = new ArrayList<String>();
		ArrayList<String> values2 = new ArrayList<String>();
		StringBuffer sb = new StringBuffer();
		sb.append("[");
		for (Text val : values) {
			sb.append(val.toString());
			sb.append(",");
			values1.add(val.toString());
			values2.add(val.toString());
			// String tag = val.toString().split("_")[0];
			// if (tag.equals("w")) weblogValues.add(val);
			// else if (tag.equals("i")) infectedValues.add(val);
		}
		sb.append("]");
		logger.info("key: " + key);
		logger.info("values: " + sb.toString());
		// logger.info("weblogValues: " + weblogValues);
		// logger.info("infectedValues: " + infectedValues);
		logger.info("values1: " + values1);
		logger.info("values2: " + values2);
		for (String val1 : values1) {
			String tag1 = val1.toString().split("_")[0];
			logger.info("tag1: " + tag1);
			for (String val2 : values2) {
				String tag2 = val2.toString().split("_")[0];
				logger.info("tag2: " + tag2);
				if (tag1.equals("w") && tag2.equals("i")) {
					String malware = val2.toString().split("_")[1];
					_malware.set(malware);
					context.write(_malware, key);
				}
			}
		}
	}
}
