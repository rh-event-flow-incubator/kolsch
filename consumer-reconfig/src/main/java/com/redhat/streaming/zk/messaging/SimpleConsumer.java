package com.redhat.streaming.zk.messaging;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SimpleConsumer extends AbstractProcessor implements Runnable {

    private static final Logger logger = Logger.getLogger(SimpleConsumer.class.getName());

    private final AtomicBoolean running = new AtomicBoolean(Boolean.TRUE);
    private KafkaConsumer<String, String> consumer;

    public SimpleConsumer() {
    }

    public void init(String kafkaUrl, String consumerGroupId, String topic) {
        super.init(topic);

        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        consumer = new KafkaConsumer<>(props);
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(Collections.singleton(topic));
            logger.info("Consumer connecting to " + topic);

            while (isRunning()) {
                final ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);

                records.forEach((ConsumerRecord<String, String> record) -> {

                    final String theValue = record.value();

                    logger.info("Received msg: " + theValue);
                });
            }
        } catch (WakeupException e) {
            // Ignore exception if closing
            if (isRunning()) {
                logger.log(Level.FINE, "Exception on close", e);
                throw e;
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "error", e);

        } finally {
            logger.info("Close the consumer.");
            consumer.close();
        }
    }


    /**
     * True when a consumer is running; otherwise false
     */
    private boolean isRunning() {
        return running.get();
    }

    /*
     * Shutdown hook which can be called from a separate thread.
     */
    public void shutdown() {
        if (isRunning()) {
            logger.info("Shutting down the consumer.");
            running.set(Boolean.FALSE);
            consumer.wakeup();
        }
    }
}
