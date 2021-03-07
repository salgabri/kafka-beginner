package com.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.logging.ConsoleHandler;

public class ConsumerDemo {
    public static void main(String[] args) {
        Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getName());
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "first-group";
        String autoOffset = "earliest"; //earliest (beginning of topic) /latest (last unread messages) /none

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffset);

        //create a new consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // subscribe consumer to topic
        consumer.subscribe(Collections.singleton("first_topic"));

        // poll for new data
        while(true) {
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord record : records) {
               log.info("key " + record.key() + ", value " + record.value());
               log.info("offset " + record.offset() + ", partition " + record.partition());
            }

        }


    }
}
