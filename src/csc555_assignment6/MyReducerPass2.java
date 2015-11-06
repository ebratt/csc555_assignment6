package csc555_assignment6;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducerPass2 extends Reducer<LongWritable, Text, Text, LongWritable>
{
  private Text _date = new Text();

  protected void reduce(LongWritable key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException
  {
    Iterator<Text> itr = values.iterator();
    while (itr.hasNext()) {
      this._date.set((Text)itr.next());
      context.write(this._date, key);
    }
  }
}