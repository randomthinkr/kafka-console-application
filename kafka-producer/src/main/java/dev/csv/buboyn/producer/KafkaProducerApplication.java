package dev.csv.buboyn.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.stream.IntStream;

public class KafkaProducerApplication {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092, localhost:9093, localhost:9094");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //send a producerRecord (message)!
        try(KafkaProducer producer = new KafkaProducer(properties)) {

            IntStream.range(0, 20).forEach(i -> {
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>("myMultiReplicationFactorTopic", "Message: " + ++i);
                producer.send(producerRecord);
             });

        }
        catch (Exception e){
            //handle properly any exception thrown.
            e.printStackTrace();
        }
    }
}
