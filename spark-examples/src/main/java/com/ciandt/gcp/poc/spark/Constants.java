package com.ciandt.gcp.poc.spark;

public class Constants {
  private static final String host = "mykafka";
  
  public static final String KAFKA_HOST_PORT = host + ":9092";
  public static final String EXAMPLE_TOPIC = "mytopic";
  public static final String ZOOKEEPER_HOST = host;
  public static final String ZK_HOST_PORT = ZOOKEEPER_HOST + ":2181";
  public static final String ZK_PATH = "/spark_examples/" + EXAMPLE_TOPIC;
  public static final String INPUT_BUCKET_PATH = "gs://<your-bucket>/path-to-xmls/";
  public static final String BQ_EXAMPLE_TABLE = "<your-project>:<dataset>.<table-name>";
  public static final String BQ_EXAMPLE_SCHEMA =
      "[{'name': 'property1', 'type': 'STRING'}, {'name': 'insertId', 'type': 'STRING'}]";
}
