# Spark POCs #

This project goal is just to provide a series of simple Spark examples (written in Java) to allow one to get started with it.

### Build ###

```mvn package```

### Run ###

```./tools/run-an-example.sh```

Some examples require that the GCS connector for hadoop is set up on the cluster. This is available by default on Dataproc, but works just fine locally if you do set it up.

Also, other examples require Kafka. Steps on how to set it up and publish messages to it in order to be consumed by the example Spark jobs are out of the scope here.

Do not forget to adjust the 'Constants.java' to point to your real bucket, Kafka and BigQuery tables.

If a local job fails you can see the error output on '<project-root>/err.tmp'

### Additional scripts ###

If you pick 'Dataproc' on the 'run-an-example' script, it assumes you have already created the dataproc cluster. You can use the following script to do that:

```./tools/dataproc-setup-cluster.sh```

If you have Kafka running on GCE VM named "mykafka", you can have it available locally by setting an alias in your '/etc/hosts', pointing "mykafka" to your localhost and then running:

```./tools/start-tunnel.sh```

## Versions ##

Tested on:
* Hadoop version 2.7.3
* gcs-connector 1.6.0
* Spark 2.1.0
* Dataproc image 1.1
* Kafka 2.11
* maven 3.5.0
* java 1.8.0
