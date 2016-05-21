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

// WordCountFour overview: It determines how many words occur once, twice, three times, and so on.
// It enters a word's count as the key and an IntWritable value of one during mapping. It then finds the  
// sum of IntWritables in the reducing phase for each word count.

public class WordCountFour {



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
      

      ArrayList<String> list = new ArrayList<String>();

      int theSize = 0;
      for (IntWritable val : values) {
	theSize += val.get();
      }

      iw.set(theSize);

      context.write(key, iw);
    
    }
  } 


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: wordcountfour <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "wordcountfour");
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
  }
}
