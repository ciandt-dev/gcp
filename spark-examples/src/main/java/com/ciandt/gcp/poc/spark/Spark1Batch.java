package com.ciandt.gcp.poc.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.ciandt.gcp.poc.spark.xml.ExampleXML;
import com.ciandt.gcp.poc.spark.xml.ParseXML;

public class Spark1Batch {

  public static void main(String[] args) {
    SparkConf sc = new SparkConf().setAppName("POC-Batch");
    try(JavaSparkContext jsc = new JavaSparkContext(sc)) {
  
      JavaRDD<ExampleXML> records = jsc.wholeTextFiles("input/")
          .map(t -> t._2())
          .map(new ParseXML());
  
      System.out.printf("Amount of XMLs: %d\n", records.count());
    }
  }
}
