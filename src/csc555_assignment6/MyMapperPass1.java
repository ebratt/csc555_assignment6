package csc555_assignment6;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapperPass1 extends Mapper<LongWritable, Text, Text, LongWritable>
{
  private Text _date = new Text();
  private LongWritable _size = new LongWritable();

  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException
  {
    String line = value.toString();

    String h1 = line.split(",")[0];
    String date = line.split(",")[2];
    String sizeString = line.split(",")[1];
    if ((!h1.equals("IP Address")) && 
      (!date.equals(null)) && 
      (!sizeString.equals(null))) {
      long size = Long.parseLong(sizeString);
      this._date.set(date);
      this._size.set(size);
      context.write(this._date, this._size);
    }
  }
}