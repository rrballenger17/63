// BIGRAM VERSION

/**
 * Illustrates a wordcount in Java
 */
package edu.hu.examples;

import java.util.*;
import java.util.Arrays;
import java.util.List;
import java.lang.Iterable;

import scala.Tuple2;

import org.apache.commons.lang.StringUtils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;


public class WordCount {
  public static void main(String[] args) throws Exception {
    String inputFile = args[0];
    String outputFile = args[1];
    // Create a Java Spark Context.
    SparkConf conf = new SparkConf().setAppName("wordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);
    // Load our input data.
    JavaRDD<String> input = sc.textFile(inputFile);
    // Split up into words.
    JavaRDD<String> words = input.flatMap(
      new FlatMapFunction<String, String>() {
        public Iterable<String> call(String x) {
          List<String> list = Arrays.asList(x.split(" "));
          List<String> cleanedList = new ArrayList<String>();


          for(int i=0; i<list.size(); i++){
            String word = list.get(i);

            // remove everything except alphanumerics and end-of-sentence punctuation 
            word = word.toLowerCase().replaceAll("[^A-Za-z0-9.?!]", "");
            
            // if word contains no alphanumerics or end-of-sentence punctuation, ignore it
            if(!word.equals("")){
              cleanedList.add(word);
            }
          }

          List<String> bigrams = new ArrayList<String>();
          String previous = null;


          for(int i=0; i<cleanedList.size(); i++){
            String word = cleanedList.get(i);
            String bigramString = null;

            if(previous != null){
              bigramString = previous + ", " + word.replaceAll("[.!?]","");
              bigrams.add(bigramString);
            }
            previous = word;

            // if words contains end-of-sentence punctuation, set previous to null
            if(!word.replaceAll("[.!?]","").equals(word)){
              previous = null;
            }
          }

          return bigrams;
        }});


    // Transform into word and count.
    JavaPairRDD<String, Integer> counts = words.mapToPair(
      new PairFunction<String, String, Integer>(){
        public Tuple2<String, Integer> call(String x){
          return new Tuple2(x, 1);
        }
      }

    ).reduceByKey(
      new Function2<Integer, Integer, Integer>(){
        public Integer call(Integer x, Integer y){ return x + y;}
      }
    );
    // Save the word count back out to a text file, causing evaluation.
    counts.saveAsTextFile(outputFile);
	}
}
