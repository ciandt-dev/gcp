package com.ciandt.gcp.poc.spark;

import static com.ciandt.gcp.poc.spark.Constants.*;

import java.util.Collections;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.ciandt.gcp.poc.spark.xml.ExampleXML;
import com.ciandt.gcp.poc.spark.xml.ParseXML;

public class Spark3Kafka {

  public static void main(String[] args) throws InterruptedException {
    SparkConf sc = new SparkConf().setAppName("POC-Kafka");
    
    try(JavaStreamingContext jsc = new JavaStreamingContext(sc, new Duration(2000))) {
      
      JavaPairReceiverInputDStream<String, String> stream = KafkaUtils.createStream(
          jsc, ZK_HOST_PORT, "a_group_id", Collections.singletonMap(EXAMPLE_TOPIC, 1));

      JavaDStream<ExampleXML> records = stream.map(t -> t._2()).map(new ParseXML());
      records.foreachRDD(rdd -> System.out.printf("Amount of XMLs: %d\n", rdd.count()));
  
      jsc.start();
      jsc.awaitTermination();
    }
  } 
}
