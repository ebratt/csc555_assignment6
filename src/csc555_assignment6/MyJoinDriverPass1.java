package csc555_assignment6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MyJoinDriverPass1 extends Configured
  implements Tool
{
  public int run(String[] args)
    throws Exception
  {
    Job job = new Job(getConf(), "MyJoinDriverPass1");

    Path in1 = new Path(args[0]);
    Path in2 = new Path(args[1]);
    Path out = new Path(args[2]);
    FileInputFormat.setInputPaths(job, in1, in2);
    FileOutputFormat.setOutputPath(job, out);
    job.setMapperClass(MyJoinMapperPass1.class);
    job.setReducerClass(MyJoinReducerPass1.class);
    job.setJarByClass(MyJoinDriverPass1.class);
    job.setNumReduceTasks(1);		// debugging
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    boolean success = job.waitForCompletion(true);
    System.exit(success ? 0 : 1);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    if (args.length != 3) {
      System.err.println("Usage: MyJoinDriverPass1.jar <in1> <in2> <out>");
      System.exit(2);
    }
    Path out = new Path(args[2]);
    FileSystem hdfs = FileSystem.get(conf);
    if (hdfs.exists(out)) {
      hdfs.delete(out, true);
    }

    int res = ToolRunner.run(conf, new MyJoinDriverPass1(), args);
    System.exit(res);
  }
}