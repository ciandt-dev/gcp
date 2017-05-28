package com.ciandt.gcp.poc.spark;

import java.util.LinkedList;
import java.util.Queue;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class Spark2Streaming {

  public static void main(String[] args) throws InterruptedException {
    SparkConf sc = new SparkConf().setAppName("POC-Streaming");
    try(JavaStreamingContext jsc = new JavaStreamingContext(sc, new Duration(2000))) {
      //JavaDStream<SampleXML> records = jsc.textFileStream("input/").map(new ParseXML());
      //textFileStream process lines of files, so xml has to be 1 line to work //alternative below

      JavaRDD<String> files = jsc.sparkContext().wholeTextFiles("input/").map(tuple -> tuple._2());
      Queue<JavaRDD<String>> rddQueue = new LinkedList<>();
      rddQueue.add(files);
      JavaDStream<String> records = jsc.queueStream(rddQueue);
  
      records.foreachRDD(rdd -> System.out.printf("Amount of XMLs: %d\n", rdd.count()));
  
      jsc.start();
      jsc.awaitTermination();
    }
  }
}
