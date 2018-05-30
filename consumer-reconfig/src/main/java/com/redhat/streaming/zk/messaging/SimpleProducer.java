package com.redhat.streaming.zk.messaging;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

public class SimpleProducer extends AbstractProcessor implements Runnable {

    private static final Logger logger = Logger.getLogger(SimpleProducer.class.getName());

    private final AtomicBoolean running = new AtomicBoolean(Boolean.TRUE);
    private KafkaProducer<String, String> producer;

    public SimpleProducer() {
    }

    public void init(String kafkaUrl, String consumerGroupId, String topic) {
        super.init(topic);

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        producer = new KafkaProducer<>(props);
    }

    @Override
    public void run() {

        int messageNo = 1;
        while (isRunning()) {
            String messageStr = "foo bar bat " + messageNo;

            try {
                producer.send(new ProducerRecord<>(topic,
                        String.valueOf(messageNo),
                        messageStr)).get();
                System.out.println("Sent message: (" + messageNo + ", " + messageStr + ")");

                Thread.sleep(2 * 1000);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                // handle the exception
            }
            ++messageNo;
        }
    }


    /**
     * True when a producer is running; otherwise false
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
        }
    }
}
