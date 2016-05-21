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

// Program overview: It sorts words by count into descending order. My program enters 
// the count in as the key in the map phase and uses a custom key sorter to get descending 
// order. It then writes out the count and the word in the reduce phase. 
public class WordCountThree {



  public static class TokenizerMapper 
       extends Mapper<Object, Text, IntWritable, Text>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {


            String line = value.toString();

            String[] parts = line.split("\\s");
            String part1 = parts[0]; // word
            int part2 = Integer.parseInt(parts[1]); // count

            // writes the count into the context as the key and the word as the value
            // this enables mapreduce to sort by word count
            context.write(new IntWritable(part2), new Text(part1));

    }
  }
  
  public static class IntSumReducer 
       extends Reducer<IntWritable, Text, IntWritable, Text> {
    
    public void reduce(IntWritable key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      

      // adds words for a particular count to an array to be sorted 
      ArrayList<String> list = new ArrayList<String>();

      for (Text val : values) {
	list.add(val.toString());
      }

      Collections.sort(list);

      // writes all the words with a count of key to the context 
     for(int i=0; i<list.size(); i++){
        context.write(key, new Text(list.get(i)));
     }
    }
  }

// custom comparable to sort in descending order
// the custom comparator reverses the sorting order of the IntWritableKeys
// so that the largest count is at the top of the output
public static class Reverse extends WritableComparator {
    protected Reverse() {
        super(IntWritable.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable one, WritableComparable two) {
        IntWritable keyOne = (IntWritable) one;
        IntWritable keyTwo = (IntWritable) two;          
        return -1 * keyOne.compareTo(keyTwo);
    }
} 


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: wordcountthree <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "wordcountthree");
    job.setJarByClass(WordCountThree.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);

    job.setSortComparatorClass(Reverse.class);

    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
