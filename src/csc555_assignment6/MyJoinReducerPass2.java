package csc555_assignment6;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyJoinReducerPass2 extends Reducer<Text, LongWritable, Text, LongWritable> {
	private LongWritable _count = new LongWritable();

	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		long sum = 0;
		for (LongWritable val : values) {
			sum += val.get();
		}
		_count.set(sum);
		context.write(key, _count);
	}

}