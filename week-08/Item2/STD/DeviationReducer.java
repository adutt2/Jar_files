import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DeviationReducer
  extends Reducer<Text, IntWritable, Text, IntWritable> {

public static double dev(List<Integer> list){
		    double total = 0.0;
		    double mean = 0.0;
		    double temp=0.0;
		    double num = 0.0;
		   

		    for (int i : list) {
		        total += i;
		    }

		    mean = total/(list.size());

		    for (int i : list) {
		         num = Math.pow(((double) i - mean), 2);
		         temp += num;
		    }

		    return Math.sqrt(temp/(list.size()));
}
  @Override
  public void reduce(Text key, Iterable<IntWritable> values,
      Context context)
      throws IOException, InterruptedException {
double Dev;
List<Integer> tempdata = new ArrayList<Integer>();
 for (IntWritable value : values) {
      tempdata.add(value.get());
    }

        Dev=dev(tempdata);

    context.write(key, new IntWritable((int)Dev));
  }
}