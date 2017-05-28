package com.ciandt.gcp.poc.spark;

import static com.ciandt.gcp.poc.spark.Constants.*;

import java.io.IOException;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.ciandt.gcp.poc.spark.xml.ExampleXML;
import com.ciandt.gcp.poc.spark.xml.ParseXML;
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.io.bigquery.BigQueryOutputFormat;
import com.google.gson.JsonObject;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class Spark6BigQuery {

  public static void main(String[] args) throws InterruptedException, IOException {
    SparkConf sc = new SparkConf().setAppName("POC-BigQuery");
    
    try(JavaStreamingContext jsc = new JavaStreamingContext(sc, new Duration(60000))) {
      JavaPairInputDStream<String, String> stream = KafkaUtils.createDirectStream(
          jsc, String.class, String.class, StringDecoder.class, StringDecoder.class,
          Collections.singletonMap("metadata.broker.list", KAFKA_HOST_PORT), Collections.singleton(EXAMPLE_TOPIC));

      Configuration conf = new Configuration();
      BigQueryConfiguration.configureBigQueryOutput(conf, BQ_EXAMPLE_TABLE, BQ_EXAMPLE_SCHEMA);
      conf.set("mapreduce.job.outputformat.class", BigQueryOutputFormat.class.getName());

      JavaDStream<ExampleXML> records = stream.map(t -> t._2()).map(new ParseXML());
      records.foreachRDD(rdd -> {
        System.out.printf("Amount of XMLs: %d\n", rdd.count());
        long time = System.currentTimeMillis();
        rdd.mapToPair(new PrepToBQ()).saveAsNewAPIHadoopDataset(conf);
        System.out.printf("Sent to BQ in %fs\n", (System.currentTimeMillis()-time)/1000f);
      });
      
      jsc.start();
      jsc.awaitTermination();
    }
  }

  @SuppressWarnings("serial")
  public static class PrepToBQ implements PairFunction<ExampleXML, String, JsonObject> {
    public Tuple2<String, JsonObject> call(ExampleXML xml) throws Exception {
      JsonObject json = new JsonObject();
      json.addProperty("property1", xml.getProperty1());
      return new Tuple2<>(null, json);
    }
  }
}
