import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgReducer
  extends Reducer<Text, IntWritable, Text, Text> {
	String temp = null;
  
  @Override
  public void reduce(Text key, Iterable<IntWritable> values,
      Context context)
      throws IOException, InterruptedException {
    double sum =0;
    double avg = 0.0;
    int maxValue = Integer.MIN_VALUE;
    int minValue = Integer.MAX_VALUE;
    for (IntWritable value : values) {
      
      maxValue = Math.max(maxValue, value.get());
      minValue = Math.min(minValue, value.get());
      sum = maxValue + minValue;
      avg = sum/2;
      String max, min, average;
      max = Integer.toString(maxValue);
      min = Integer.toString(minValue);
      average = Double.toString(avg);
      temp = max + " " + min + " " + average ;
      
    }
    context.write(key, new Text(temp));
  }
}