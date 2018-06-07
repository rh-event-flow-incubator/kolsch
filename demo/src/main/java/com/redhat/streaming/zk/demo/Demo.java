package com.redhat.streaming.zk.demo;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

public class Demo {
    private static final Logger logger = Logger.getLogger(Demo.class.getName());

    private static final String zkUrl = "localhost:2181";
    private static final String path = "/streams/app1";


    public static void main(String... args) {

        try {
            logger.info("Starting Demo");

            CuratorFramework client = CuratorFrameworkFactory.newClient(zkUrl, new ExponentialBackoffRetry(1000, 3));
            client.start();

            client.createContainers(path + "/producer/kafkaUrl");
            client.setData().forPath(path + "/producer/kafkaUrl", "localhost:9092".getBytes());

            client.createContainers(path + "/producer/topic");
            client.setData().forPath(path + "/producer/topic", "topic1".getBytes());

            client.createContainers(path + "/processor/in/kafkaUrl");
            client.setData().forPath(path + "/processor/in/kafkaUrl", "localhost:9092".getBytes());

            client.createContainers(path + "/processor/in/topic");
            client.setData().forPath(path + "/processor/in/topic", "topic1".getBytes());

            client.createContainers(path + "/processor/out/kafkaUrl");
            client.setData().forPath(path + "/processor/out/kafkaUrl", "localhost:9092".getBytes());

            client.createContainers(path + "/processor/out/topic");
            client.setData().forPath(path + "/processor/out/topic", "topic2".getBytes());

            client.createContainers(path + "/consumer/kafkaUrl");
            client.setData().forPath(path + "/consumer/kafkaUrl", "localhost:9092".getBytes());

            client.createContainers(path + "/consumer/topic");
            client.setData().forPath(path + "/consumer/topic", "topic2".getBytes());


            client.close();

            final ExecutorService executor = Executors.newFixedThreadPool(3);

//            ZKSingleTopicWrapper zkProducer= new ZKSingleTopicWrapper(zkUrl, path + "/producer",  new SimpleProducer());
//            executor.submit(zkProducer);
//
//            List<String> nodes = Arrays.asList("/processor/in", "/processor/out");
//            ZKMultiTopicWrapper zkProcessor = new ZKMultiTopicWrapper(zkUrl, path, nodes, new SimpleProcessor());
//            executor.submit(zkProcessor);
//
//
//            ZKSingleTopicWrapper zkConsumer = new ZKSingleTopicWrapper(zkUrl, path + "/consumer", new SimpleConsumer());
//            executor.submit(zkConsumer);
//
//
//            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//                executor.shutdown();
//                try {
//                    executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
//                } catch (InterruptedException ie) {
//                    logger.log(Level.SEVERE, "Error on close", ie);
//                }
//            }));
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
