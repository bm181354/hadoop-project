package QuestionThree;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class QuestionThree {
	
  private static final String OUTPUT_PATH = "intermediatePath";	
/*
 * JOB 1 :-  Mapper and reducer	classes
 */
	
///////////////////////////////////////////////////////////////////////////////////
	
  public static class MapperOne 
       extends Mapper<Object, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }
///////////////////////////////////////////////////////////////////////////////////
  public static class ReducerOne 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
  ///////////////////////////////////////////////////////////////////////////////////
  /*
   * Job 2 :- Mapper and Reducer 
   */
  
  public static class MapperTwo 
  extends Mapper< LongWritable, Text, Text, IntWritable>{

	  private final static IntWritable one = new IntWritable(1);
	  
	  private Text word = new Text();

	  public void map(LongWritable key, Text value, Context context
			  ) throws IOException, InterruptedException {
		  
		  StringTokenizer itr = new StringTokenizer(value.toString());
		  
		  
		  while (itr.hasMoreTokens()) {
			  
			  String title = itr.nextToken(); 
			  String num = itr.nextToken();
			  
			  word.set(num);
			  context.write(word, one);
		  }
		  
		  
	  }
  } 
  ///////////////////////////////////////////////////////////////////////////////////
  public static class ReducerTwo 
  extends Reducer<Text,IntWritable,Text,IntWritable> {
	  private IntWritable result = new IntWritable();

	  public void reduce(Text key, Iterable<IntWritable> values, 
			  Context context
			  ) throws IOException, InterruptedException {
		  int sum = 0;
		  for (IntWritable val : values) {
			  sum += val.get();
		  }
		  result.set(sum);
		  context.write(key, result);
	  }
  }
  ///////////////////////////////////////////////////////////////////////////////////
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 3) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    
    /*JOB 1*/
    Job job = new Job(conf, "Job_one");
    job.setJarByClass(QuestionThree.class);
    
    job.setMapperClass(MapperOne.class);
    
    job.setCombinerClass(ReducerOne.class);
    job.setReducerClass(ReducerOne.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    job.waitForCompletion(true);
    
    
    System.out.println("++++++++++++++++++++++++++++++++++");
    
    /*JOB 2*/
    
    Job jobTwo = new Job(conf, "Job_two");
    jobTwo.setJarByClass(QuestionThree.class);
    
    jobTwo.setMapperClass(MapperTwo.class);
    jobTwo.setCombinerClass(ReducerTwo.class);
    jobTwo.setReducerClass(ReducerTwo.class);
    
    jobTwo.setOutputKeyClass(Text.class);
    jobTwo.setOutputValueClass(IntWritable.class);
    
    FileInputFormat.addInputPath(jobTwo, new Path(otherArgs[1]));
    FileOutputFormat.setOutputPath(jobTwo, new Path(otherArgs[2]));

    
    System.exit(jobTwo.waitForCompletion(true) ? 0 : 1);
    
    System.out.println("===============================");
  }
}
///////////////////////////////////////////////////////////////////////////////////
