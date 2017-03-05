import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MaxTemperature_chaining {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperature <input path> <output path>");
      System.exit(-1);
    }
  Configuration conf = getConf();
  FileSystem fs = FileSystem.get(conf);
  Job job = new Job(conf, "Job1");
    job.setJarByClass(MaxTemperature_chaining.class);
    job.setJobName("Max temperature_chaining");
    
    job.setMapperClass(MaxTemperatureMapper1.class);
    job.setReducerClass(MaxTemperatureReducer1.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
 
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class)
    
TextInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
  TextOutputFormat.setOutputPath(job2, new Path(args[1]));

    job.waitForCompletion(true);
    Job job2 = new Job(conf, "Job1");
    job.setJarByClass(MaxTemperature_chaining.class);
    job.setJobName("Max temperature_chaining");
    
    job.setMapperClass(MaxTemperatureMapper2.class);
    job.setReducerClass(MaxTemperatureReducer2.class);

job2.setInputFormatClass(TextInputFormat.class);
  job2.setOutputFormatClass(TextOutputFormat.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

TextInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
  TextOutputFormat.setOutputPath(job2, new Path(args[1]));
    
 return job2.waitForCompletion(true) ? 0 : 1;
  }
}