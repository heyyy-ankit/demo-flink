package org.example;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;

public class UserClickCounter {

    public static void main(String[] args) throws Exception {

        // Step 1: Setup environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Step 2: Create input stream (simulate events)
        DataStream<Tuple2<String, String>> clicks = env.fromElements(
            Tuple2.of("user1", "click"),
            Tuple2.of("user2", "click"),
            Tuple2.of("user1", "click"),
            Tuple2.of("user3", "click"),
            Tuple2.of("user2", "click"),
            Tuple2.of("user1", "click")
        );

        // Step 3: Key by userId
        clicks
            .keyBy(t -> t.f0)
            .map(new ClickCountingFunction()) // stateful function
            .print();

        // Step 4: Execute
        env.execute("User Click Counter with State");
    }

    // Step 5: Define a stateful function
    public static class ClickCountingFunction extends RichMapFunction<Tuple2<String, String>, String> {

        // This will store count per user
        private transient ValueState<Integer> clickCount;

        @Override
        public void open(Configuration configuration) {
            ValueStateDescriptor<Integer> descriptor =
                new ValueStateDescriptor<>("clickCount", Integer.class);
            clickCount = getRuntimeContext().getState(descriptor);
        }

        @Override
        public String map(Tuple2<String, String> value) throws Exception {
            Integer currentCount = clickCount.value();
            if (currentCount == null) currentCount = 0;

            currentCount += 1; // increment count
            clickCount.update(currentCount); // save updated count

            return value.f0 + " has clicked " + currentCount + " times.";
        }
    }
}
