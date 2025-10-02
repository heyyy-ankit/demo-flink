package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class AdvancedWordCount {
    public static void main(String[] args) throws Exception {
      try (StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment()) {
        
        // Source: read from socket
        DataStream<String> text = env.socketTextStream("localhost", 9999);
        
        // Stopwords set
        Set<String> stopWords = new HashSet<>(Arrays.asList(
            "a", "the", "is", "in", "at", "of", "on", "and", "or", "to", "with"
        ));
        
        DataStream<Tuple2<String, Integer>> counts = text
            // Normalize case + remove punctuation
            .map((MapFunction<String, String>) value -> value.toLowerCase().replaceAll("[^a-z\\s]", ""))
            // Tokenize
            .flatMap(new Tokenizer())
            // Filter stopwords
            .filter((FilterFunction<Tuple2<String, Integer>>) value -> !stopWords.contains(value.f0))
            // Group by word
            .keyBy((KeySelector<Tuple2<String, Integer>, String>) value -> value.f0)
            // Apply sliding window: size = 10s, slide = 5s
//            .window(SlidingProcessingTimeWindows.of(Duration.ofSeconds(10), Duration.ofSeconds(5)))
            // Aggregate counts
            .sum(1);
        
        // Print results
        counts.print();
        
        env.execute("Advanced Streaming WordCount");
      }
    }

    // Tokenizer function
    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            for (String word : value.split("\\s+")) {
                if (word.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        }
    }
}
