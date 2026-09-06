package io.jtsolv.kafka.developer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

public class MultiPartitionConsumer {

    public static void main(String[] args) {
        // Kafka consumer configuration
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");  // Kafka server address
        properties.put("group.id", "test-group");  // Consumer group ID
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());
        properties.put("auto.offset.reset", "earliest");  // Start reading from the earliest message

        // Create a KafkaConsumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        String topicName = "jtsolv-test-001";
        // Assign specific partitions manually (e.g., partitions 0 and 1 of the "test-topic")
        consumer.assign(Arrays.asList(
                new TopicPartition(topicName, 0),  // Partition 0 of "test-topic"
                new TopicPartition(topicName, 1)   // Partition 1 of "test-topic"
        ));

        // Poll messages from multiple partitions
        while (true) {
            // Poll for messages
            consumer.poll(1000).forEach(record -> {
                // Print the consumed message and partition number
                System.out.println("Consumed message: " + record.value() + " from partition: " + record.partition() + " at offset: " + record.offset());
            });
        }
    }
}
