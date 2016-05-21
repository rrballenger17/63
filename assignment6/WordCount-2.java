package edu.hu.examples;

import java.util.Arrays;

import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCount {
	public static void main(String[] args) throws Exception {
		String inputFile = args[0];
        String inputFileTwo = args[1];
		String outputFile = args[2];
        String outputFileTwo = args[3];
        
		// Create a Java Spark Context.
		SparkConf conf = new SparkConf().setAppName("wordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load our input data.
		JavaRDD<String> paragraphA = sc.textFile(inputFile);
        JavaRDD<String> paragraphB = sc.textFile(inputFileTwo);
        
        
        // RDDs that contain only words
		paragraphA = paragraphA.flatMap(new FlatMapFunction<String, String>() {
			public Iterable<String> call(String x) {
				List<String> words = Arrays.asList(x.split(" "));
                for(int i=0; i<words.size(); i++){
                    String word = words.get(i).replaceAll("[^A-Za-z0-9]", "").toLowerCase();
                    words.set(i, word);
                }
                return words;
            }
		});
        
        paragraphB = paragraphB.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String x) {
                List<String> words = Arrays.asList(x.split(" "));
                for(int i=0; i<words.size(); i++){
                    String word = words.get(i).replaceAll("[^A-Za-z0-9]", "").toLowerCase();
                    words.set(i, word);
                }
                return words;
            }
        });
        
        // unique words in each paragraph
        JavaRDD<String> uniqueParagraphA = paragraphA.distinct();
        JavaRDD<String> uniqueParagraphB = paragraphB.distinct();
        
        // present in paragraph A but not in paragraph B
        JavaRDD<String> paraAandNotB = paragraphA.subtract(paragraphB).distinct();
        
        // only words common in both paragraphs
        JavaRDD<String> commonWords = paragraphA.intersection(paragraphB).distinct();
        
        
		// Save the word count back out to a text file, causing evaluation.
		paragraphA.saveAsTextFile("output/allWordsA");
        paragraphB.saveAsTextFile("output/allWordsB");
        
        uniqueParagraphA.saveAsTextFile("output/uniqueA");
        uniqueParagraphB.saveAsTextFile("output/uniqueB");
        
        paraAandNotB.saveAsTextFile("output/A_not_B");
        
        commonWords.saveAsTextFile("output/common");
	}

}
