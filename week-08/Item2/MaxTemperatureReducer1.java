import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTemperatureReducer1
  extends Reducer<Text, IntWritable, Text, IntWritable> {
ArrayList<Integer> temperatureList = new ArrayList<Integer>();
	String temp = null;
  
  @Override
  public void reduce(Text key, Iterable<IntWritable> values,
      Context context)
      throws IOException, InterruptedException {
    
    int median = 0;
		for (IntWritable value : values) {
			temperatureList.add(value.get());
		}
		Collections.sort(temperatureList);
		int size  = temperatureList.size();

		if(size%2 == 0){
			int half = size/2;

			median  = temperatureList.get(half);
		}else {
			int half = (size + 1)/2;
			median = temperatureList.get(half -1);
		}
		context.write(key, new IntWritable(median));
	}

}