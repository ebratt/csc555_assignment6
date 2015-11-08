package csc555_assignment6;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyJoinReducerPass2 extends Reducer<Text, Text, Text, LongWritable> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		LongWritable _count = new LongWritable();
		HashSet<Text> _set = new HashSet<Text>();
		for (Text val : values) _set.add(val);
		_count.set(_set.size());
		context.write(key, _count);
		_count = null;
		_set = null;
	}

}