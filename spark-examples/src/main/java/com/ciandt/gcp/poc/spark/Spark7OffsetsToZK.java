package com.ciandt.gcp.poc.spark;

import static com.ciandt.gcp.poc.spark.Constants.*;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.ZkClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.AbstractJavaRDDLike;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.apache.spark.util.LongAccumulator;

import com.ciandt.gcp.poc.spark.xml.ExampleXML;
import com.ciandt.gcp.poc.spark.xml.ParseXML;
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.io.bigquery.BigQueryOutputFormat;
import com.google.gson.JsonObject;

import kafka.common.TopicAndPartition;
import kafka.serializer.StringDecoder;
import kafka.utils.ZkUtils;
import scala.Option;
import scala.Tuple2;

/**
 * This implementation achieves at-least-once semantics. Because, in case of
 * failure, we may re-process the last batch but since in this example we just
 * pipe the XML to BQ, without any aggregation or anything fancy and the output
 * to BQ is idempotent (with the help of insertId for consumers) this is
 * effectively a exactly-once semantic.
 */
@SuppressWarnings("serial")
public class Spark7OffsetsToZK {

  public static void main(String[] args) throws InterruptedException, IOException {
    SparkConf sc = new SparkConf().setAppName("POC-OffsetsToZK");

    try (JavaStreamingContext jsc = new JavaStreamingContext(sc, new Duration(60000))) {

      LongAccumulator stopCondition = jsc.ssc().sc().longAccumulator();
      JavaPairDStream<String, String> stream = dealWithOffsets(jsc);
      
      final ParseXML parseXML = new ParseXML();
      JavaPairDStream<String,ExampleXML> records = stream.mapToPair(
          tuple -> new Tuple2<>(tuple._1(), parseXML.call(tuple._2())));

      Configuration conf = new Configuration();
      BigQueryConfiguration.configureBigQueryOutput(conf, BQ_EXAMPLE_TABLE, BQ_EXAMPLE_SCHEMA);
      conf.set("mapreduce.job.outputformat.class", BigQueryOutputFormat.class.getName());

      records.foreachRDD(rdd -> {
        System.out.printf("Amount of XMLs: %d\n", rdd.count());
        if (rdd.count() > 0L) {
          stopCondition.reset();
          long time = System.currentTimeMillis();
          rdd.mapToPair(new PrepToBQ()).saveAsNewAPIHadoopDataset(conf);
          System.out.printf("Sent to BQ in %fs\n", (System.currentTimeMillis() - time) / 1000f);
        } else {
          stopCondition.add(1L);
          if (stopCondition.value() >= 2L)
            jsc.stop();
        }
      });

      jsc.start();
      jsc.awaitTermination();
    }
  }

  private static JavaPairDStream<String, String> dealWithOffsets(JavaStreamingContext jsc) {
    Option<String> offsets = ZkUtils.readDataMaybeNull(new ZkClient(ZOOKEEPER_HOST), ZK_PATH)._1();
    return offsets.isDefined() ? startFromOffsets(jsc, offsets.get()) : startNewStream(jsc);
  }

  private static JavaPairDStream<String, String> startFromOffsets(JavaStreamingContext jsc, String offsetsInput) {
    Map<TopicAndPartition, Long> map = new HashMap<>();
    for (String partition : offsetsInput.split(",")) {
      String[] offset = partition.split(":");
      map.put(new TopicAndPartition(EXAMPLE_TOPIC, Integer.parseInt(offset[0])), Long.parseLong(offset[1]));
    }

    JavaDStream<String> stream = KafkaUtils.createDirectStream(jsc, String.class, String.class, StringDecoder.class,
        StringDecoder.class, String.class, Collections.singletonMap("metadata.broker.list", KAFKA_HOST_PORT), map,
        msg -> msg.message());
    
    return stream.transformToPair(new ToPairWithOffset<>(str -> str));
  }
  
  private static JavaPairDStream<String, String> startNewStream(JavaStreamingContext jsc) {
    JavaPairInputDStream<String, String> stream = KafkaUtils.createDirectStream(jsc, String.class, String.class,
        StringDecoder.class, StringDecoder.class, Collections.singletonMap("metadata.broker.list", KAFKA_HOST_PORT),
        Collections.singleton(EXAMPLE_TOPIC));

    return stream.transformToPair(new ToPairWithOffset<>(tuple -> tuple._2()));
  }

  public static class ToPairWithOffset<E, T extends AbstractJavaRDDLike<E,?>> implements Function<T, JavaPairRDD<String, String>> {
    final Function<E, String> getContent;
    
    ToPairWithOffset(Function<E, String> getContent) { this.getContent = getContent; }
    
    public JavaPairRDD<String, String> call(T rdd) throws Exception {
      final OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
      StringBuilder offsetsStr = new StringBuilder();
      for (OffsetRange offset : offsets)
        offsetsStr.append(',').append(offset.partition()).append(':').append(offset.fromOffset());
      ZkUtils.updatePersistentPath(new ZkClient(ZOOKEEPER_HOST), ZK_PATH, offsetsStr.substring(1));
      
      return rdd.mapPartitionsWithIndex((idx, ite) -> {
        OffsetRange offset = offsets[idx];
        List<Tuple2<String, String>> list = new LinkedList<>();
        for (int i = 0; ite.hasNext(); ++i)
          list.add(new Tuple2<>(idx + "-" + (offset.fromOffset() + i), getContent.call(ite.next())));
        return list.iterator();
      }, true).mapPartitionsToPair(ite -> ite);
    }
  }
  
  public static class PrepToBQ implements PairFunction<Tuple2<String, ExampleXML>, String, JsonObject> {
    public Tuple2<String, JsonObject> call(Tuple2<String, ExampleXML> tuple) throws Exception {
      JsonObject json = new JsonObject();
      json.addProperty("property1", tuple._2().getProperty1());
      json.addProperty("insertId", tuple._1());
      return new Tuple2<>(null, json);
    }
  }
}
