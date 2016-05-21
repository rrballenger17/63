package org.apache.hadoop.examples;

import java.util.*;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.fs.FileSystem;


// Chained programs from problems #1 and #3 
// The map and reduce classes for the problem #1 program are at the bottom 
// of this file. The map and reduce classes for the counting of words by number of 
// occurrences (problem #3) are at the top. 
public class WordCountChained {



  public static class TokenizerMapper 
       extends Mapper<Object, Text, IntWritable, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {


            String line = value.toString();

            String[] parts = line.split("\\s");
            String part1 = parts[0]; // word
            int part2 = Integer.parseInt(parts[1]); // count
            
            context.write(new IntWritable(part2), new IntWritable(1));

    }
  }
  
  public static class IntSumReducer 
       extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
   
    private IntWritable iw = new IntWritable();

    public void reduce(IntWritable key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      
	// key is count
	// iterable is word

      ArrayList<String> list = new ArrayList<String>();

      int theSize = 0;
      for (IntWritable val : values) {
	theSize += val.get();
      }

      iw.set(theSize);

      context.write(key, iw);
    
    }
  } 
  

 private void cleanup(Path temp, Configuration conf) throws IOException{
    FileSystem fs = temp.getFileSystem(conf);
    fs.delete(temp, true);
 }


 // Run job #1 and put output into a temp folder named chain-temp.
 // Then run job #2 and using the previous output at input.
 // lastly, clean up by deleting the temporary files. 
  public int run(Configuration conf, String[] args) throws Exception{

     Path in = new Path(args[0]);
     Path out = new Path(args[1]);
     Path temp = new Path("chain-temp");
     Job job1 = createJob1(conf, in, temp);
     //JobClient.runJob(job1);
     job1.waitForCompletion(true);

     Job job2 = createJob2(conf, temp, out);
    // JobClient.runJob(job2);
     
      int forReturn = job2.waitForCompletion(true) ? 1: 0;
      cleanup(temp, conf);

      return forReturn;
  //   return 0;
  }

  private Job createJob1(Configuration conf, Path in, Path out) throws IOException{
    Job job = Job.getInstance(conf, "wordcountchained_part1");

    job.setJarByClass(WordCountChained.class);
 
    job.setMapperClass(TokenizerMapperOne.class);
    job.setCombinerClass(IntSumReducerOne.class);
    job.setReducerClass(IntSumReducerOne.class);
 
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
   // for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, in);
   // }
    FileOutputFormat.setOutputPath(job, out);

   // System.exit(job.waitForCompletion(true) ? 0 : 1);
   return job;
  }

  private Job createJob2(Configuration conf, Path in, Path out) throws IOException{
     Job job = Job.getInstance(conf, "wordcountchained");
    job.setJarByClass(WordCountChained.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);

   // for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, in);
   // }
    FileOutputFormat.setOutputPath(job, out);
    return job;

  }



  public static void main(String[] args) throws Exception {
    /*Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: wordcountfour <in> [<in>...] <out>");
      System.exit(2);
    }
    */

   // int res = ToolRunner.run(new Configuration(), new WordCountChained(), args);
    WordCountChained var = new WordCountChained();

    int res = var.run(new Configuration(), args);

    System.exit(res);
   


    /*Job job = Job.getInstance(conf, "wordcountfour");
    job.setJarByClass(WordCountFour.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);

    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    */

  }






























  public static class TokenizerMapperOne
       extends Mapper<Object, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {


      List<String> stops = Arrays.asList("i", "a", "about", "an", "are", "as", "at", "be", "by", "com", "for", "from", "how", "in", "is", "it", "of", "on", "or", "that", "the", "this", "to", "was", "what", "when", "where", "who", "will", "with", "the", "www");


      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());



        boolean letters = word.toString().matches(".*[a-zA-Z]+.*");

        boolean numbers = word.toString().matches(".*[0-9]+.*");
        

        if(letters || numbers){
          if(!stops.contains(word.toString().toLowerCase())){
            context.write(word, one);
          }
        }
        
      }
    }
  }
  
  public static class IntSumReducerOne
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
/*
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: wordcounttwo <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "wordcounttwo");
    job.setJarByClass(WordCountTwo.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
*/


}
