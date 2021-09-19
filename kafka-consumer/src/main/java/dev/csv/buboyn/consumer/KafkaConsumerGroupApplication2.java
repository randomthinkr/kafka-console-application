package dev.csv.buboyn.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Properties;

public class KafkaConsumerGroupApplication2 {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092, localhost:9093, localhost:9094");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "test-consumer-group");

        //consume a message!
        try (KafkaConsumer consumer = new KafkaConsumer(properties)) {
            consumer.subscribe(List.of("myMultiReplicationFactorTopic"));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.of(10, ChronoUnit.MILLIS));
                records.forEach(record -> System.out.println("Message received: " + record.value()));
            }

        } catch (Exception e) {
            //handle properly any exception thrown.
            e.printStackTrace();
        }
    }
}
