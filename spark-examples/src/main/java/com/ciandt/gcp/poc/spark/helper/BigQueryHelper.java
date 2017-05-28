package com.ciandt.gcp.poc.spark.helper;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;

import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.io.bigquery.BigQueryOutputFormat;
import com.google.gson.JsonObject;

public class BigQueryHelper {
  
  public static <X> VoidFunction<JavaPairRDD<X, JsonObject>> outputTo(String table, String schema) throws IOException {
    Configuration conf = new Configuration();
    conf.set("mapreduce.job.outputformat.class", BigQueryOutputFormat.class.getName());
    BigQueryConfiguration.configureBigQueryOutput(conf, table, schema);

    return rdd -> {
      if (rdd.count() > 0L) {
        long time = System.currentTimeMillis();
        /* This was only required the first time on a fresh table, it seems I had to kickstart the _PARTITIONTIME pseudo-column
         * but now it automatically add to the proper table using ingestion time. Using the decorator would only be required
         * if we were to place the entries using their "event timestamp", e.g. loading rows on old partitions.
         * Implementing that would be much harder though, since'd have to check each message, or each "partition" (date-based)
        if (partitioned) {
          String today = ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyyMMdd"));
          BigQueryConfiguration.configureBigQueryOutput(conf, table + "$" + today, schema);
        }*/
        rdd.saveAsNewAPIHadoopDataset(conf);
        System.out.printf("Sent %d rows to BQ in %.1fs\n", rdd.count(), (System.currentTimeMillis() - time) / 1000f);
      }
    };
  }
}
