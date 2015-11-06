package csc555_assignment6;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class MyJoinReducerPass1 extends Reducer<Text, Text, Text, Text> {
	private Text _malware = new Text();
	private Logger logger = Logger.getLogger(MyJoinReducerPass1.class);
	private ArrayList<String> weblogValues = new ArrayList<String>();
	private ArrayList<Text> infectedValues = new ArrayList<Text>();

	/**
	 * @param key:
	 *            example 192.0.0.1
	 * @param values:
	 *            example [i_TrojanHorse,i_SpyEye,w_,w_,i_Zbot]
	 */
	protected void reduce(Text key, Iterator<Text> values, Context context) throws IOException, InterruptedException {

		logger.info("key: " + key.toString());
		boolean weblogFlag = false;

		while (values.hasNext()) {
			Text val = values.next();
			String source = val.toString().split("_")[0];
			if (source.equals("w"))
				weblogValues.add(val.toString());
			else
				infectedValues.add(val);
			source = null;
		}

		logger.info("weblogValues: " + weblogValues.toString());
		logger.info("infectedValues: " + infectedValues.toString());

		if (weblogValues.size() > 0)
			weblogFlag = true;

		if (weblogFlag) {
			for (Text iv : infectedValues) {
				logger.info("iv.toString(); " + iv.toString());
				String malware = iv.toString().split("_")[1];
				this._malware.set(malware);
				context.write(_malware, key);
			}
		}
		weblogValues.clear();
		infectedValues.clear();
	}
}
