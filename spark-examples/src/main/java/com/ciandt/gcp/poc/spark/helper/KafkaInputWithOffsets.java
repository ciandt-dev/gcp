package com.ciandt.gcp.poc.spark.helper;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.ZkClient;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;

import com.google.common.collect.ImmutableMap;

import kafka.common.TopicAndPartition;
import kafka.serializer.StringDecoder;
import kafka.utils.ZkUtils;
import scala.Option;
import scala.Tuple2;

/**
 * Creates a Kafka input DStream starting from the offset where it last stopped.
 * Or, if starting fresh, the oldest message available on Kafka. Offsets are
 * stored in Zookeeper.
 * 
 * One key difference from KafkaUtils.createDirectStream is that the
 * JavaPairDStream returned here sets the message offset as first parameter on
 * the pair instead of the (mostly useless) message key. Message content still
 * features as 2nd parameter.
 * 
 * RDDs' partitions follow those of the Kafka topic, but RDDs are transformed
 * and no longer carry `HasOffsetRanges` interface. Not that it's useful now,
 * since the offsets are on the 1st parameter.
 * 
 * This method only guarantees at-least-once semantics. To achieve exactly-once
 * your aggregations and output must be idempotent.
 */
public class KafkaInputWithOffsets implements Serializable {
  private static final long serialVersionUID = 1L;
  
  public final String kafka;
  public final String topic;
  public final String zookeeper;
  public final String zkPath;

  public KafkaInputWithOffsets(String kafkaHostPort, String topic,
      String zookeeperHost, String zkPath) {
    this.kafka = kafkaHostPort;
    this.topic = topic;
    this.zookeeper = zookeeperHost;
    this.zkPath = zkPath;
  }

  public JavaPairDStream<String, String> createResumableStream(JavaStreamingContext jsc) {
    Option<String> offsets = ZkUtils.readDataMaybeNull(new ZkClient(zookeeper), zkPath)._1();
    return offsets.isDefined() ? startFromOffsets(jsc, offsets.get()) : startNewStream(jsc);
  }

  private JavaPairDStream<String, String> startNewStream(JavaStreamingContext jsc) {
    JavaPairInputDStream<String, String> stream = KafkaUtils.createDirectStream(
        jsc, String.class, String.class, StringDecoder.class, StringDecoder.class,
        ImmutableMap.of("metadata.broker.list", kafka, "auto.offset.reset", "smallest"),
        Collections.singleton(topic));

    return stream.transformToPair(new ToPairWithOffsets<>(tuple -> tuple._2()));
  }

  private JavaPairDStream<String, String> startFromOffsets(JavaStreamingContext jsc, String offsets) {
    Map<TopicAndPartition, Long> map = new HashMap<>();
    for (String offset : offsets.split(",")) {
      String[] split = offset.split(":");
      map.put(new TopicAndPartition(topic, Integer.parseInt(split[0])), Long.parseLong(split[1]));
    }

    JavaDStream<String> stream = KafkaUtils.createDirectStream(
        jsc, String.class, String.class, StringDecoder.class, StringDecoder.class, String.class,
        Collections.singletonMap("metadata.broker.list", kafka), map, msg -> msg.message());

    return stream.transformToPair(new ToPairWithOffsets<>(str -> str));
  }

  private class ToPairWithOffsets<E, T extends JavaRDDLike<E, ?>>
      implements Function<T, JavaPairRDD<String, String>> {
    private static final long serialVersionUID = 1L;

    final Function<E, String> getContent;

    ToPairWithOffsets(Function<E, String> getContent) {
      this.getContent = getContent;
    }

    @Override
    public JavaPairRDD<String, String> call(T rdd) throws Exception {
      final OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
      StringBuilder offsetsStr = new StringBuilder();
      for (OffsetRange offset : offsets)
        offsetsStr.append(',').append(offset.partition()).append(':').append(offset.fromOffset());
      ZkUtils.updatePersistentPath(new ZkClient(zookeeper), zkPath, offsetsStr.substring(1));

      return rdd.mapPartitionsWithIndex((idx, ite) -> {
        OffsetRange offset = offsets[idx];
        List<Tuple2<String, String>> list = new LinkedList<>();
        for (int i = 0; ite.hasNext(); ++i)
          list.add(new Tuple2<>(idx + "-" + (offset.fromOffset() + i), getContent.call(ite.next())));
        return list.iterator();
      }, true).mapPartitionsToPair(ite -> ite);
    }
  }
}
