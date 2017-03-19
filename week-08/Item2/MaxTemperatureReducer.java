
// Reducer to find Maximum and Minimum Temperatures

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTemperatureReducer
  extends Reducer<Text, IntWritable, Text, Text> {
	String temp = null;
  
  @Override
  public void reduce(Text key, Iterable<IntWritable> values,
      Context context)
      throws IOException, InterruptedException {
    
    int maxValue = Integer.MIN_VALUE;
    int minValue = Integer.MAX_VALUE;
    for (IntWritable value : values) {
      maxValue = Math.max(maxValue, value.get());
      minValue = Math.min(minValue, value.get());
      String max, min;
      max = Integer.toString(maxValue);
      min = Integer.toString(minValue);
      temp = max + " " + min;
      
    }
    context.write(key, new Text(temp));
  }
}
