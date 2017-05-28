package com.ciandt.gcp.poc.spark;

import static com.ciandt.gcp.poc.spark.Constants.*;

import java.util.Collections;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.ciandt.gcp.poc.spark.xml.ExampleXML;
import com.ciandt.gcp.poc.spark.xml.ParseXML;

import kafka.serializer.StringDecoder;

public class Spark4KafkaNew {

  public static void main(String[] args) throws InterruptedException {
    SparkConf sc = new SparkConf().setAppName("POC-Kafka-New");
    
    try(JavaStreamingContext jsc = new JavaStreamingContext(sc, new Duration(2000))) {
      
      JavaPairInputDStream<String, String> stream = KafkaUtils.createDirectStream(
          jsc, String.class, String.class, StringDecoder.class, StringDecoder.class,
          Collections.singletonMap("metadata.broker.list", KAFKA_HOST_PORT),
          Collections.singleton(EXAMPLE_TOPIC));

      JavaDStream<ExampleXML> records = stream.map(t -> t._2()).map(new ParseXML());
      records.foreachRDD(rdd -> System.out.printf("Amount of XMLs: %d\n", rdd.count()));
  
      jsc.start();
      jsc.awaitTermination();
    }
  } 
}
