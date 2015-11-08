package csc555_assignment6;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyJoinMapperPass2 extends Mapper<LongWritable, Text, Text, Text> {
	private Text _malware = new Text();
	private Text _address = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		_malware.set(line.split("\\t")[0]);
		_address.set(line.split("\\t")[1]);
		context.write(_malware, _address);
	}
}