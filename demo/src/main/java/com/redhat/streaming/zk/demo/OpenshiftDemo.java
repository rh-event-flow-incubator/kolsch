package com.redhat.streaming.zk.demo;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.logging.Logger;

public class OpenshiftDemo {
    private static final Logger logger = Logger.getLogger(OpenshiftDemo.class.getName());

    private static final String zkUrl = "my-cluster-zookeeper:2181";
    private static final String path = "/streams";


    public static void main(String... args) {

        try {
            logger.info("Starting Demo");

            CuratorFramework client = CuratorFrameworkFactory.newClient(zkUrl, new ExponentialBackoffRetry(1000, 3));
            client.start();

            client.createContainers(path + "/producer/kafkaUrl");
            client.setData().forPath(path + "/producer/kafkaUrl", "my-cluster-kafka:9092".getBytes());

            client.createContainers(path + "/producer/topic");
            client.setData().forPath(path + "/producer/topic", "topic1".getBytes());

            client.createContainers(path + "/processor/in/kafkaUrl");
            client.setData().forPath(path + "/processor/in/kafkaUrl", "my-cluster-kafka:9092".getBytes());

            client.createContainers(path + "/processor/in/topic");
            client.setData().forPath(path + "/processor/in/topic", "topic1".getBytes());

            client.createContainers(path + "/processor/out/kafkaUrl");
            client.setData().forPath(path + "/processor/out/kafkaUrl", "my-cluster-kafka:9092".getBytes());

            client.createContainers(path + "/processor/out/topic");
            client.setData().forPath(path + "/processor/out/topic", "topic2".getBytes());

            client.createContainers(path + "/consumer/kafkaUrl");
            client.setData().forPath(path + "/consumer/kafkaUrl", "my-cluster-kafka:9092".getBytes());

            client.createContainers(path + "/consumer/topic");
            client.setData().forPath(path + "/consumer/topic", "topic3".getBytes());

            client.close();

        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
