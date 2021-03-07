package com.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    public static void main(String[] args) {

        Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        // create producer properties
        Properties properties = new Properties();
        String bootstrapServer = "127.0.0.1:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // create a producer record
        String topicName = "first_topic";
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, "message " + i);

            // send data - asynchronous
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        log.info("Received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        log.error("ERROR", e);
                    }
                }
            });
        }
        //flush data
        producer.flush();

    }
}
