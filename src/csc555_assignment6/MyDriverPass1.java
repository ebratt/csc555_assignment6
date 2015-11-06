package csc555_assignment6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MyDriverPass1 extends Configured
  implements Tool
{
  public int run(String[] args)
    throws Exception
  {
    Job job = new Job(getConf(), "MyDriverPass1");

    Path in = new Path(args[0]);
    Path out = new Path(args[1]);
    FileInputFormat.setInputPaths(job, in);
    FileOutputFormat.setOutputPath(job, out);
    job.setMapperClass(MyMapperPass1.class);
    job.setReducerClass(LongSumReducer.class);
    job.setCombinerClass(LongSumReducer.class);
    job.setJarByClass(MyDriverPass1.class);
    job.setNumReduceTasks(1);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    boolean success = job.waitForCompletion(true);
    System.exit(success ? 0 : 1);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    if (args.length != 2) {
      System.err.println("Usage: MyDriverPass1.jar <in> <out>");
      System.exit(2);
    }
    Path out = new Path(args[1]);
    FileSystem hdfs = FileSystem.get(conf);
    if (hdfs.exists(out)) {
      hdfs.delete(out, true);
    }

    int res = ToolRunner.run(conf, new MyDriverPass1(), args);
    System.exit(res);
  }
}