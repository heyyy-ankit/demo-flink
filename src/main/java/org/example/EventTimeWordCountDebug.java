package org.example;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class EventTimeWordCountDebug {

    public static class Event {
        public long timestamp;
        public String word;

        public Event() {}
        public Event(long timestamp, String word) {
            this.timestamp = timestamp;
            this.word = word;
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Source: "timestamp,word"
        DataStream<String> input = env.socketTextStream("localhost", 9999);

        DataStream<Event> events = input.map(line -> {
            String[] parts = line.split(",");
            long ts = Long.parseLong(parts[0].trim());
            String word = parts[1].trim();
            return new Event(ts, word);
        });

        // --- Custom WatermarkStrategy with logging ---
        WatermarkStrategy<Event> wmStrategy = new WatermarkStrategy<Event>() {
            @Override
            public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context ctx) {
                return (event, recordTs) -> event.timestamp; // extract event timestamp
            }
            
            @Override
            public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context ctx) {
                return new WatermarkGenerator<Event>() {
                    private final long maxOutOfOrderness = 5000; // 5s
                    private long maxTsSeen = Long.MIN_VALUE;
                    
                    @Override
                    public void onEvent(Event event, long eventTs, WatermarkOutput output) {
                        maxTsSeen = Math.max(maxTsSeen, eventTs);
                    }
                    
                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {
                        if (maxTsSeen != Long.MIN_VALUE) { // emit only after first event
                            long wm = maxTsSeen - maxOutOfOrderness;
                            System.out.println(">>> Emitting Watermark: " + wm);
                            output.emitWatermark(new Watermark(wm));
                        }
                    }
                };
            }
        };

        DataStream<Event> withWatermarks = events.assignTimestampsAndWatermarks(wmStrategy);

        // Debug step: print each event with watermark
        withWatermarks.process(new org.apache.flink.streaming.api.functions.ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event event, Context ctx, Collector<Event> out) {
                long currentWM = ctx.timerService().currentWatermark();
                System.out.println("Event(" + event.word + ", ts=" + event.timestamp + ") | Current WM=" + currentWM);
                out.collect(event);
            }
        });

        // Tokenize into (word,1)
        DataStream<Tuple2<String, Integer>> words = withWatermarks.flatMap(
            new FlatMapFunction<Event, Tuple2<String, Integer>>() {
                @Override
                public void flatMap(Event e, Collector<Tuple2<String, Integer>> out) {
                    out.collect(new Tuple2<>(e.word.toLowerCase(), 1));
                }
            });

        // Side output for too-late events
        final OutputTag<Tuple2<String, Integer>> lateOutput =
                new OutputTag<Tuple2<String, Integer>>("late-data"){};

        // Tumbling 10s event-time windows
        SingleOutputStreamOperator<Tuple2<String, Integer>> counts = words
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.of(10, TimeUnit.SECONDS)))
                .allowedLateness(Time.of(5, TimeUnit.SECONDS))
                .sideOutputLateData(lateOutput)
                .sum(1);
        
        counts.print("WINDOW_COUNTS");
        counts.getSideOutput(lateOutput).print("TOO_LATE");

        env.execute("Event Time WordCount with Watermark Debug");
    }
}
