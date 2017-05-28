package Q4b;


import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q4 {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
	  
    
    private final static IntWritable one = new IntWritable(1);
    
    private int RESET = 0;
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    
    	
    	
      int counter = 0;
      StringTokenizer itr = new StringTokenizer(value.toString());
      
      
      while (itr.hasMoreTokens()) {
          String letter = itr.nextToken();
          
          
          letter.replaceAll(",","");
          letter.replaceAll(".","");
          letter.replaceAll(":","");
          letter.replaceAll(" ","");

          int lengthofStr = letter.length(); //[GOOD]
          
          for(int i = 0; i< (lengthofStr);i++){
        	  
        	  if (isVowel(letter.charAt(i))){
        		  counter++;
        		  
        	  }
          }
          //////////////////////
          /*String Conversion*/
          String numOfVowelToString = String.valueOf(counter); //{ good }
          String lengthOfStr = String.valueOf(lengthofStr); //{ good }

           //Text conversion
           Text length = new Text(lengthOfStr); 
           
           //IntWritable Conversion
           IntWritable numOfVowel =  new IntWritable(Integer.parseInt(numOfVowelToString));	
           //+++++++++++++++++++++++++++++++++++
           
           System.out.println("++++++++++++++");
           System.out.println(length);
           System.out.println(numOfVowel);
           System.out.println("++++++++++++++");
           
           
           // Send in the length of the word and number of vowel in a word.
           context.write(length, numOfVowel);
          
          //++++++++++++++++++++++++
          ///////////////////////  
          counter = RESET;
      }
    }
    
    public boolean isVowel(char l){
    	
    	char ll = Character.toLowerCase(l);
    	
    	if (ll == 'a' || ll == 'e' ||ll == 'i' ||ll == 'o' ||ll == 'u' ){
    		return true;
    	}
    	else
    		return false;
    }
  }
  
  ///////////////////////////////////////////////////////////////////////
  
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
    	
    	int index = 0;
    	
    	String lengthOfWord = key.toString();
    	System.out.println("===============");
    	System.out.println(key);
    	System.out.println("===============");
    	int length = Integer.parseInt(lengthOfWord);
    	
    	
    	for(index = 0; index < length ; index++ )
    	{
    		
    		String concatination_text = length +""+index; 
    		//String fraction_Str = Integer.toString(length+index);
    		Text word = new Text();
    		word.set(concatination_text);
    		
    		result = fraction(index,values);
    		
    		context.write(word,result); // break
    		
    	}
    	
    	
    }
     
    IntWritable fraction(int index, Iterable<IntWritable> values){
    	
    	 int v_counter = 0;
    	 int length = 0;
    	 int num = 0;
    	 
    	 /////////////////////////
    	 
    	 for(IntWritable value : values)
    	 {
    		 num = value.get();
    		 
    		 System.out.println("///////////////");
    	     System.out.println(num); //(good)
    	     System.out.println(index); //
    	     System.out.println("///////////////");
    		 
    		 if (index == num ){
    			 
    			 v_counter++;
    			
    		 }
    		 length++;
    	 }
    	 
    	 System.out.println("&&&&&&&&&&&&&&");
	     System.out.println(v_counter);
	     System.out.println("&&&&&&&&&&&&&&&");
    	 
    	 int fraction = (v_counter / (length + 1));
    	 
    	 String fraction_Str = Integer.toString(fraction);
    	 
    	 IntWritable fraction_IntWritable =  new IntWritable(Integer.parseInt(fraction_Str));
    	 return fraction_IntWritable;
    	 
    	 //////////////////////
    	 
     }
     
    
    
   
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    
    Job job = new Job(conf, "Question4b");
    job.setJarByClass(Q4.class);
    
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}



	


