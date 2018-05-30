package com.redhat.streaming.zk.demo;

import com.redhat.streaming.zk.messaging.SimpleConsumer;
import com.redhat.streaming.zk.messaging.SimpleProcessor;
import com.redhat.streaming.zk.messaging.SimpleProducer;
import com.redhat.streaming.zk.wrapper.ZKMultiTopicWrapper;
import com.redhat.streaming.zk.wrapper.ZKSingleTopicWrapper;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Demo {
    private static final Logger logger = Logger.getLogger(Demo.class.getName());

    private static final String zkUrl = "localhost:2181";
    private static final String path = "/streams/app1";
    private static final String prodNode = "/prod";
    private static final String consNode = "/cons";

    private static final String kafkaUrl = "localhost:9092";
    private static final String consumerGroupId = "group1";

    public static void main(String... args) {

        logger.info("Starting Demo");
        final ExecutorService executor = Executors.newSingleThreadExecutor();

        ZKSingleTopicWrapper zkProducer= new ZKSingleTopicWrapper(zkUrl, path, prodNode, new SimpleProducer(), kafkaUrl, "group1");
        executor.submit(zkProducer);

        List<String> nodes = Arrays.asList("/procIn", "/procOut");
        ZKMultiTopicWrapper zkProcessor = new ZKMultiTopicWrapper(zkUrl, path, nodes, new SimpleProcessor(), kafkaUrl, "group2");
        executor.submit(zkProcessor);


        ZKSingleTopicWrapper zkConsumer = new ZKSingleTopicWrapper(zkUrl, path, consNode, new SimpleConsumer(), kafkaUrl, "group3");
        executor.submit(zkConsumer);


        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            executor.shutdown();
            try {
                executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ie) {
                logger.log(Level.SEVERE, "Error on close", ie);
            }
        }));


    }
}
