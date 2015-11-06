package csc555_assignment6;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapperPass2 extends Mapper<LongWritable, Text, LongWritable, Text>
{
  private Text _date = new Text();
  private LongWritable _size = new LongWritable();

  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException
  {
    String line = value.toString();
    String date = line.split("\\t")[0];
    String sizeString = line.split("\\t")[1];
    if ((!date.equals(null)) && 
      (!sizeString.equals(null))) {
      long size = Long.parseLong(sizeString);
      this._date.set(date);
      this._size.set(size);
      context.write(this._size, this._date);
    }
  }
}