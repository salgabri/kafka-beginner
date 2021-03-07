package com.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoThread {
    public static void main(String[] args) {
        new ConsumerDemoThread().run();
    }

    private ConsumerDemoThread() {

    }

    private void run() {
        Logger log = LoggerFactory.getLogger(ConsumerDemoThread.class.getName());

        CountDownLatch latch = new CountDownLatch(1);
        Runnable myConsumerRunnable = new ConsumerRunnable(latch);
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // shutdown
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            log.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            log.info("Application has exited");
        }));
        try {
            latch.await();
        } catch (InterruptedException e) {
            log.info("Application got interrupted");
        } finally {
            log.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {

        Logger log = LoggerFactory.getLogger(ConsumerDemoThread.class.getName());
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(CountDownLatch latch) {
            this.latch = latch;

            String bootstrapServers = "127.0.0.1:9092";
            String groupId = "thread-group";
            String autoOffset = "earliest"; //earliest (beginning of topic) /latest (last unread messages) /none

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffset);

            //create a new consumer
            this.consumer = new KafkaConsumer<String, String>(properties);

            // subscribe consumer to topic
            consumer.subscribe(Collections.singleton("first_topic"));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord record : records) {
                        log.info("key " + record.key() + ", value " + record.value());
                        log.info("offset " + record.offset() + ", partition " + record.partition());
                    }

                }
            } catch(WakeupException e) {
                log.info("Received shutdown signal");
            } finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown() {
            consumer.wakeup();
        }
    }
}
