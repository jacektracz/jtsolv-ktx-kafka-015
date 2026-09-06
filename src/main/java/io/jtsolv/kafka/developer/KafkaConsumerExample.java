package io.jtsolv.kafka.developer;

// KafkaConsumerExample.java
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerExample {

    public static void main(String[] args) {
        // Set Kafka consumer properties
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092"); // Kafka server
        properties.put("group.id", "jtsolv-group-2"); // Consumer group
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());
        properties.put("auto.offset.reset", "earliest"); // Start reading from the earliest message

        // Create the Kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Subscribe to the topic
        String topic = "t-1";
        consumer.subscribe(Collections.singletonList(topic));

        // Poll messages from the topic
        int ii = 0;
        while (true) {
            long milliseconds = 1000;
            dbg("Consume in loop number:" + ii);
            dbg("Consumer pool start wait for milisecons:" + ii);
            consumer.poll(milliseconds).forEach(record -> {
                dbg("Consumed message: " + record.value() );
                dbg("Consumed from partition: " + record.partition());
                dbg("Consumed offset: " + record.offset());
                dbg("Consumed key: " + record.key());
                dbg("Consumed groupId: " + consumer.groupMetadata().groupId());
                dbg("Consumed memberId: " + consumer.groupMetadata().memberId());
                dbg("Consumed generationId: " + consumer.groupMetadata().generationId());
            });
            ii++;
            dbg("Sleep start:");
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            dbg("Sleep end:");

        }
    }

    private static void dbg(String txt){
        System.out.println(txt);
    }
}
