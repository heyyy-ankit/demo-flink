package org.example;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class UserEventListState {

    public static void main(String[] args) throws Exception {

        // Step 1: Setup environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Step 2: Create a stream of user events
        DataStream<Tuple2<String, String>> userEvents = env.fromElements(
                Tuple2.of("user1", "login"),
                Tuple2.of("user2", "view"),
                Tuple2.of("user1", "click"),
                Tuple2.of("user2", "logout"),
                Tuple2.of("user1", "purchase")
        );

        // Step 3: Key by userId and apply stateful function
        userEvents
            .keyBy(t -> t.f0)
            .map(new UserEventCollector())
            .print();

        env.execute("ListState Example");
    }

    // Step 4: Define stateful function
    public static class UserEventCollector extends RichMapFunction<Tuple2<String, String>, String> {

        private transient ListState<String> eventList;

        @Override
        public void open(Configuration configuration) {
            ListStateDescriptor<String> descriptor =
                new ListStateDescriptor<>("userEvents", String.class);
            eventList = getRuntimeContext().getListState(descriptor);
        }

        @Override
        public String map(Tuple2<String, String> value) throws Exception {
            // Add new event to the state list
            eventList.add(value.f1);

            // Convert ListState (Iterable) to a normal List so we can print it
            List<String> allEvents = new ArrayList<>();
            for (String e : eventList.get()) {
                allEvents.add(e);
            }

            return value.f0 + " has events: " + allEvents;
        }
    }
}
