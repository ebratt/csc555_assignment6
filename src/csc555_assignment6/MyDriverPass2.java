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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MyDriverPass2 extends Configured
  implements Tool
{
  public int run(String[] args)
    throws Exception
  {
    Job job = new Job(getConf(), "MyDriverPass2");

    Path in = new Path(args[0]);
    Path out = new Path(args[1]);
    FileInputFormat.setInputPaths(job, in);
    FileOutputFormat.setOutputPath(job, out);
    job.setMapperClass(MyMapperPass2.class);
    job.setReducerClass(MyReducerPass2.class);
    job.setJarByClass(MyDriverPass2.class);

    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    boolean success = job.waitForCompletion(true);
    System.exit(success ? 0 : 1);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    if (args.length != 2) {
      System.err.println("Usage: MyDriverPass2.jar <in> <out>");
      System.exit(2);
    }
    Path out = new Path(args[1]);
    FileSystem hdfs = FileSystem.get(conf);
    if (hdfs.exists(out)) {
      hdfs.delete(out, true);
    }

    int res = ToolRunner.run(conf, new MyDriverPass2(), args);
    System.exit(res);
  }
}