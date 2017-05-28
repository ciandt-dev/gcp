package com.ciandt.gcp.poc.spark;

import static com.ciandt.gcp.poc.spark.Constants.*;

import java.io.IOException;

import javax.xml.bind.JAXBException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.ciandt.gcp.poc.spark.helper.BigQueryHelper;
import com.ciandt.gcp.poc.spark.helper.IdleStop;
import com.ciandt.gcp.poc.spark.helper.KafkaInputWithOffsets;
import com.ciandt.gcp.poc.spark.xml.ExampleXML;
import com.ciandt.gcp.poc.spark.xml.ParseXML;
import com.google.gson.JsonObject;

import scala.Tuple2;

/**
 * Based on Spark7OffsetsToZK, but resilient to bad inputs.
 * And slightly more organized :)
 */
public class Spark8Organized {

  public static void main(String[] args) throws InterruptedException, IOException, JAXBException {
    SparkConf sc = new SparkConf().setAppName("Receiving-KafkaToBQ");

    try (JavaStreamingContext jsc = new JavaStreamingContext(sc, new Duration(60000))) {

      JavaPairDStream<String, String> stream = new KafkaInputWithOffsets(
          KAFKA_HOST_PORT, EXAMPLE_TOPIC, ZOOKEEPER_HOST, ZK_PATH).createResumableStream(jsc);

      stream.foreachRDD(IdleStop.create(jsc, 2, "XMLs count: %d\n"));

      stream
          .mapToPair(parseXml())
          .filter(t -> t != null)
          .mapToPair(prepToBq())
          .foreachRDD(BigQueryHelper.outputTo(BQ_EXAMPLE_TABLE, BQ_EXAMPLE_SCHEMA));

      jsc.start();
      jsc.awaitTermination();
    }
  }

  private static PairFunction<Tuple2<String, String>, String, ExampleXML> parseXml() {
    ParseXML parser = new ParseXML();
    return tuple -> {
      try {
        return new Tuple2<>(tuple._1(), parser.call(tuple._2()));
      } catch(JAXBException badXML) {
        System.err.printf("Bad XML at %s\n", tuple._1());
        badXML.printStackTrace();
        return null;
      }
    };
  }
  
  private static PairFunction<Tuple2<String, ExampleXML>, Object, JsonObject> prepToBq() {
    return tuple -> {
      JsonObject json = new JsonObject();
      json.addProperty("property1", tuple._2().getProperty1());
      json.addProperty("insertId", tuple._1());
      return new Tuple2<>(null, json);
    };
  }
}
