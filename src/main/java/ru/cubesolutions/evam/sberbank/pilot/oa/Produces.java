package ru.cubesolutions.evam.sberbank.pilot.oa;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by Garya on 27.11.2017.
 */
public class Produces {

    public static void main(String[] argv) throws Exception {
        String topicName = "topicc";
        String message = "message message";

        //Configure the Producer
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        org.apache.kafka.clients.producer.Producer producer = new KafkaProducer<String, String>(configProperties);

        ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topicName, message);
        producer.send(rec);

        producer.close();
    }

}