package csc555_assignment6;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyJoinMapperPass2 extends Mapper<Text, Text, Text, LongWritable> {
	private LongWritable _one = new LongWritable(1);

	public void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
		context.write(key, _one);
	}
}