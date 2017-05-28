package com.ciandt.gcp.poc.spark;

import static com.ciandt.gcp.poc.spark.Constants.INPUT_BUCKET_PATH;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.ciandt.gcp.poc.spark.xml.ExampleXML;
import com.ciandt.gcp.poc.spark.xml.ParseXML;

public class Spark5BatchGCS {

  public static void main(String[] args) {
    SparkConf sc = new SparkConf().setAppName("POC-Batch-GCS");
    try(JavaSparkContext jsc = new JavaSparkContext(sc)) {
  
      JavaRDD<ExampleXML> records = jsc.wholeTextFiles(INPUT_BUCKET_PATH)
          .map(t -> t._2())
          .map(new ParseXML());
  
      System.out.printf("Amount of XMLs: %d\n", records.count());
    }
  }
}
