package io.jtsolv.kafka.developer;

// KafkaProducerExample.java
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerExample {

    public static void main(String[] args) {
        // Set Kafka producer properties
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092"); // Kafka server
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());

        // Create the Kafka producer
        Producer<String, String> producer = new KafkaProducer<>(properties);

        // Produce a message to Kafka topic
        //String topic = "jtsolv-test-001";
        String topic = "t-1";
        String initialKey = "key_24_onto_5_partitions_";
        String initialValue = "Hello Kafka By Jacek Tracz (JTSOLV)!:";
        int numberOfSend = 100;
        for (int ii =0 ; ii< numberOfSend; ii++ ) {
            String key = initialKey + ii;
            String value = initialValue + ii + "--" + key;
            sendValue(producer, topic, key, value);
        }
        // Send a record (message)

        // Close the producer
        producer.close();
    }

    private static void sendValue(Producer<String, String> producer,
                                  String topic,
                                  String key,
                                  String value) {
        dbg("Before send message: [key:" + key + "]");
        dbg("Before send message: [value:" + value + "]");
        producer.send(new ProducerRecord<>(topic, key, value), (metadata, exception) -> {
            if (exception != null) {
                dbg("Error while producing message: " + exception.getMessage());
            } else {
                dbg("Message sent successfully.");
                dbg("Partition: " + metadata.partition());
                dbg("Topic: " + metadata.topic());
                dbg("Offset: " + metadata.offset());
                dbg("Timestamp: " + metadata.timestamp());
                dbg("Message: " + value);
                dbg("HasTimestamp: " + metadata.hasTimestamp());
            }
        });
    }

    private static void dbg(String txt){
        System.out. println(txt);
    }
}
