package io.jtsolv.kafka.developer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

public class ManualPartitionAssignmentConsumer {

    public static void main(String[] args) {
        // Set Kafka consumer properties
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092"); // Kafka server
        properties.put("group.id", "test-group"); // Consumer group ID
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());
        properties.put("auto.offset.reset", "earliest");

        // Create Kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Assign specific partitions manually
        consumer.assign(Arrays.asList(
                new TopicPartition("test-topic", 0), // Partition 0 of "test-topic"
                new TopicPartition("test-topic", 1)  // Partition 1 of "test-topic"
        ));

        // Poll messages in an infinite loop
        while (true) {
            consumer.poll(1000).forEach(record -> {
                System.out.println("Consumed message: " + record.value() + " from partition: " + record.partition());
            });
        }
    }
}
