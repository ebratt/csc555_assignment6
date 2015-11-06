package csc555_assignment6;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MyJoinMapperPass1 extends Mapper<LongWritable, Text, Text, Text> {
	private Text _ipaddress = new Text();
	private Text _w = new Text("w_");
	private Text _infected = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

		String line = value.toString();
		String h1 = line.split(",")[0];
		String ipaddress = line.split(",")[0];
		if ((!h1.equals("IP Address")) && (!ipaddress.equals(null))) {
			this._ipaddress.set(ipaddress);
			if (fileName.equals("weblog.txt")) {
				context.write(this._ipaddress, this._w);
			}
			if (fileName.equals("infected.txt")) {
				String malware = line.split(",")[1];
				if (!malware.equals(null)) {
					this._infected.set("i_" + malware);
					context.write(this._ipaddress, this._infected);
				}
			}
		}
	}
}