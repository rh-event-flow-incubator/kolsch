package com.redhat.streaming.zk.messaging;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

public class SimpleProcessor extends AbstractProcessor implements Runnable {

    private static final Logger logger = Logger.getLogger(SimpleProcessor.class.getName());

    private final AtomicBoolean running = new AtomicBoolean(Boolean.TRUE);
    private KafkaStreams streams;
    private CountDownLatch latch;
    private Properties props;

    private List<String> topics = new ArrayList<>();


    public SimpleProcessor() {
    }

    public void init(String kafkaUrl, String consumerGroupId, String path, List<String> nodes, Map<String, String> nodeTopicMapping) {

        for(String node : nodes){
            topics.add(nodeTopicMapping.get(path + node));
        }

        props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, consumerGroupId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    }

    @Override
    public void run() {

        if(topics.size() != 2){
            System.exit(1);
        }
        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source = builder.stream(topics.get(0));
        source.flatMapValues(value -> Arrays.asList(value.split("\\W+")))
                .to(topics.get(1));

        final Topology topology = builder.build();
        streams = new KafkaStreams(topology, props);
        latch = new CountDownLatch(1);

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
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
            logger.info("Shutting down the processor.");
            if(streams != null){
                streams.close();
            }
            if(latch != null){
                latch.countDown();
            }

        }
    }
}
