package org.example.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.functions.ProcessFunction;

import java.util.Iterator;
import java.util.Map;

public class TradeChangeDetectionJob {
    
    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Configure Kafka source
        KafkaSource<String> source = KafkaSource.<String>builder()
//            .setBootstrapServers("localhost:9092")
            .setBootstrapServers("broker:29092")
            .setTopics("trades")
            .setGroupId("trade-change-detector")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();
        
        // Create data stream from Kafka
        DataStream<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        
        // Process trades and detect changes
        DataStream<String> processedStream = kafkaStream
            .keyBy(new TradeKeySelector())
            .process(new TradeChangeProcessor());
        
        // Print results (in production, you'd write to another Kafka topic or database)
        processedStream.print();
        
        // Execute the job
        env.execute("Trade Change Detection Job 2");
    }
    
    // Key selector to partition by trade ID
    public static class TradeKeySelector implements KeySelector<String, String> {
        private final ObjectMapper mapper = new ObjectMapper();
        
        @Override
        public String getKey(String trade) throws Exception {
            JsonNode jsonNode = mapper.readTree(trade);
            return jsonNode.get("id").asText();
        }
    }
    
    // Process function to detect changes
    public static class TradeChangeProcessor extends ProcessFunction<String, String> {
        private final ObjectMapper mapper = new ObjectMapper();
        
        // State to store the previous trade for each ID
        private transient ValueState<String> previousTradeState;
        
        @Override
        public void open(Configuration configuration) throws Exception {
            ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<>(
                "previousTrade",
                TypeInformation.of(new TypeHint<String>() {}));
            previousTradeState = getRuntimeContext().getState(descriptor);
        }
        
        @Override
        public void processElement(String currentTrade, Context context, Collector<String> out) throws Exception {
            JsonNode currentJson = mapper.readTree(currentTrade);
            String tradeId = currentJson.get("id").asText();
            
            // Get previous trade from state
            String previousTrade = previousTradeState.value();
            
            if (previousTrade == null) {
                // First time seeing this trade - just store it
                previousTradeState.update(currentTrade);
                System.out.println("First occurrence of trade ID: " + tradeId);
            } else {
                // Compare with previous trade and find differences
                JsonNode previousJson = mapper.readTree(previousTrade);
                ObjectNode changes = findDifferences(previousJson, currentJson);
                
                if (changes.size() > 0) {
                    // Add metadata to the changes
                    changes.put("tradeId", tradeId);
                    changes.put("timestamp", System.currentTimeMillis());
                    changes.put("changeType", "TRADE_UPDATE");
                    
                    // Output the changes
                    out.collect(changes.toString());
                    
                    // Update state with current trade
                    previousTradeState.update(currentTrade);
                } else {
                    System.out.println("No changes detected for trade ID: " + tradeId);
                }
            }
        }
        
        private ObjectNode findDifferences(JsonNode previous, JsonNode current) {
            ObjectNode differences = mapper.createObjectNode();
            
            // Compare all fields in current JSON
            Iterator<Map.Entry<String, JsonNode>> fields = current.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                String fieldName = entry.getKey();
                JsonNode currentValue = entry.getValue();
                JsonNode previousValue = previous.get(fieldName);
                
                // Check if field is new or changed
                if (previousValue == null) {
                    ObjectNode change = mapper.createObjectNode();
                    change.put("action", "ADDED");
                    change.set("newValue", currentValue);
                    differences.set(fieldName, change);
                } else if (!previousValue.equals(currentValue)) {
                    ObjectNode change = mapper.createObjectNode();
                    change.put("action", "MODIFIED");
                    change.set("oldValue", previousValue);
                    change.set("newValue", currentValue);
                    differences.set(fieldName, change);
                }
            }
            
            // Check for removed fields
            Iterator<Map.Entry<String, JsonNode>> previousFields = previous.fields();
            while (previousFields.hasNext()) {
                Map.Entry<String, JsonNode> entry = previousFields.next();
                String fieldName = entry.getKey();
                if (!current.has(fieldName)) {
                    ObjectNode change = mapper.createObjectNode();
                    change.put("action", "REMOVED");
                    change.set("oldValue", entry.getValue());
                    differences.set(fieldName, change);
                }
            }
            
            return differences;
        }
    }
}