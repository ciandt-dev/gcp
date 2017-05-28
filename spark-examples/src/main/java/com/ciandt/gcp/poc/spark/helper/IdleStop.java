package com.ciandt.gcp.poc.spark.helper;

import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.util.LongAccumulator;

public class IdleStop {
  
  public static <A extends JavaRDDLike<?, ?>> VoidFunction<A> create(JavaStreamingContext jsc, long amount, String printf) {
    final LongAccumulator stopAcc = jsc.ssc().sc().longAccumulator();
    return rdd -> {
      if (printf != null)
        System.out.printf(printf, rdd.count());
      if (rdd.count() == 0L) {
        stopAcc.add(1L);
        if (stopAcc.value() >= amount)
          jsc.stop();
      } else
        stopAcc.reset();
    };
  }
}
