package org.example;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

// Simple POJO representing a user transaction
public class FraudDetectorExample {

    // POJO must be public and have a no-arg constructor
    public static class Transaction {
        public String userId;
        public double amount;
        public long timestamp;

        public Transaction() {}

        public Transaction(String userId, double amount, long timestamp) {
            this.userId = userId;
            this.amount = amount;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "Transaction(" + userId + ", $" + amount + ", ts=" + timestamp + ")";
        }
    }

    // Our main function — executes the job
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Sample transaction stream
        List<Transaction> transactions = List.of(
                new Transaction("user1", 120.0, 1000L),
                new Transaction("user1", 250.0, 2000L),
                new Transaction("user1", 300.0, 2500L),
                new Transaction("user2", 50.0, 3000L),
                new Transaction("user1", 200.0, 4000L),
                new Transaction("user1", 500.0, 15000L), // new alert window
                new Transaction("user2", 700.0, 16000L),
                new Transaction("user2", 800.0, 18000L),
                new Transaction("user2", 900.0, 19000L)
        );

        DataStreamSource<Transaction> source = env.fromCollection(transactions);

        SingleOutputStreamOperator<String> alerts = source
                .keyBy(tx -> tx.userId)
                .flatMap(new FraudDetector());

        alerts.print();

        env.execute("Fraud Detection with ListState + ValueState");
    }

    // The main processing logic
    public static class FraudDetector extends RichFlatMapFunction<Transaction, String> {

        private transient ListState<Transaction> recentTransactions;
        private transient ValueState<Long> lastAlertTime;

        @Override
        public void open(Configuration configuration) throws Exception {
            ListStateDescriptor<Transaction> listDescriptor =
                    new ListStateDescriptor<>("recentTransactions", Transaction.class);
            recentTransactions = getRuntimeContext().getListState(listDescriptor);

            ValueStateDescriptor<Long> valueDescriptor =
                    new ValueStateDescriptor<>("lastAlertTime", Long.class);
            lastAlertTime = getRuntimeContext().getState(valueDescriptor);
        }

        @Override
        public void flatMap(Transaction tx, Collector<String> out) throws Exception {
            System.out.println("Processing: " + tx);

            // Only track high-value transactions
            if (tx.amount > 100) {
                recentTransactions.add(tx);
            }

            // Remove transactions older than 10 minutes
            List<Transaction> filtered = new ArrayList<>();
            long cutoff = tx.timestamp - 10 * 60 * 1000L;

            for (Transaction t : recentTransactions.get()) {
                if (t.timestamp >= cutoff) {
                    filtered.add(t);
                }
            }
            recentTransactions.update(filtered);

            // Check if there are 3 or more recent high-value transactions
            if (filtered.size() >= 3) {
                Long lastAlert = lastAlertTime.value();

                // Trigger alert if last alert was long ago or none yet
                if (lastAlert == null || tx.timestamp - lastAlert > 10 * 60 * 1000L) {
                    String alert = String.format(
                            "🚨 ALERT: Suspicious pattern for user=%s, recentHighValueTxns=%d, currentTx=%s",
                            tx.userId, filtered.size(), tx);
                    out.collect(alert);
                    lastAlertTime.update(tx.timestamp);
                }
            }
        }
    }
}
