import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
 
public class MaxTemperature_chaining {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperature <input path> <output 

path>");
      System.exit(-1);
    }
    
    Configuration conf = getConf();
    JobConf job = new JobConf(true);
    job.setJarByClass(MaxTemperature_chaining.class);
    job.setJobName("Max temperature");

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    JobConf map1Conf = new JobConf(false);
ChainMapper.addMapper(job,
                      MaxTemperatureMapper.class,
                      LongWritable.class,
                      Text.class,
                      Text.class,
                      IntWritable.class,
                      true,
                      map1Conf);
JobConf map2Conf = new JobConf(false);
ChainMapper.addMapper(job,
                      MaxTemperatureMapper2.class,
                      LongWritable.class,
                      Text.class,
                      Text.class,
                      IntWritable.class,
                      true,
                      map2Conf);
JobConf reduceConf2 = new JobConf(false);
ChainReducer.setReducer(job,
                        MaxTemperatureReducer2.class,
                        Text.class,
                        IntWritable.class,
                        Text.class,
                        IntWritable.class,
                        true,
                        reduceConf2);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
