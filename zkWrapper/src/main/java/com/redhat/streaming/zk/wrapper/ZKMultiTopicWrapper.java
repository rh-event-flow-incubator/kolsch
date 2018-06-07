package com.redhat.streaming.zk.wrapper;

import com.redhat.streaming.zk.messaging.AbstractProcessor;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Logger;

/**
 *
 * */
public class ZKMultiTopicWrapper implements Runnable {

    private static final Logger logger = Logger.getLogger(ZKMultiTopicWrapper.class.getName());

    //ZK Setup
    private String zkUrl;
    private String path;
    private List<String> nodes;
    private Map<String, KafkaConfig> nodeMapping = new HashMap<>();

    //Kafka Setup
    private AbstractProcessor processor;

    //Handle on the thread we start
    private Future f = null;

    public ZKMultiTopicWrapper(String zkUrl, String path, List<String> nodes, AbstractProcessor processor) {  //}, String kafkaUrl, String consumerGroupId) {
        this.zkUrl = zkUrl;
        this.path = path;
        this.nodes = nodes;
        this.processor = processor;
        for (String node : nodes) {
            nodeMapping.put(path + node, new KafkaConfig());
        }
    }

    @Override
    public void run() {

        CuratorFramework client;
        TreeCache cache;

        try {
            //Zookeeper setup using Curator
            client = CuratorFrameworkFactory.newClient(zkUrl, new ExponentialBackoffRetry(1000, 3));
            client.start();

            // Use a Path cache to allow multiple nodes in future
            cache = TreeCache.newBuilder(client, path).setCacheData(true).build();
            cache.start();
            addListener(cache);

            //Initial stream setup if the nodes exists in ZK
            for (String nodePlusPath : nodeMapping.keySet()) {

                try {
                    String urlData = new String(client.getData().forPath(nodePlusPath + "/kafkaUrl"));
                    nodeMapping.get(nodePlusPath).setKafkaUrl(urlData);
                } catch (Exception ignored) {

                }

                try {
                    String topicData = new String(client.getData().forPath(nodePlusPath + "/topic"));
                    nodeMapping.get(nodePlusPath).setKafkaTopic(topicData);
                } catch (Exception ignored) {

                }
            }

            boolean configComplete = true;
            for (KafkaConfig config : nodeMapping.values()) {
                if (!config.isValid()) {
                    configComplete = false;
                }
            }

            if (configComplete) {
                startProcessor();
            } else {
                logger.info("Not connected to topic");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private void startProcessor() {

        try {

            String clazzName = processor.getClass().getCanonicalName();

            // Shut down existing processor if it exists
            stopProcessor();

            // Create the consumer thread
            final ExecutorService executor = Executors.newSingleThreadExecutor();

            Class clazz = Class.forName(clazzName);
            processor = (AbstractProcessor) clazz.newInstance();

            List<String> topics = new ArrayList<>();
            for (String key : nodes) {
                topics.add(nodeMapping.get(path + key).getKafkaTopic());
            }
            //At the moment we only support getting a single Kafka so get the URL from the first node
            processor.init(nodeMapping.values().iterator().next().getKafkaUrl(), topics);
            f = executor.submit(processor);

            StringBuilder nodeList = new StringBuilder();
            for (String node : nodes) {
                nodeList.append(nodeMapping.get(path + node)).append(", ");
            }

            logger.info("Started thread to connect to Topics: " + nodeList.substring(0, nodeList.length() - 2));
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            e.printStackTrace();
        }
    }

    /**
     * Terminate the thread
     */
    private void stopProcessor() {

        if (f != null) {
            logger.info("Consumer disconnecting");
            processor.shutdown();
            //todo: check that the consumer thread dies
        }
    }

    /**
     * Add a listener to get updates from the cache for the given children of the path.
     *
     * @param cache Cache to get notifications from
     */
    private void addListener(TreeCache cache) {

        TreeCacheListener listener = (client, event) -> {

            switch (event.getType()) {
                case NODE_ADDED: {

                    logger.fine("Node added: " + ZKPaths.getNodeFromPath(event.getData().getPath()));

                    //Connect
                    connect(event);

                    break;
                }

                case NODE_UPDATED: {
                    logger.fine("Node changed: " + ZKPaths.getNodeFromPath(event.getData().getPath()) + ". New value: " + new String(event.getData().getData()));

                    //Reconnect
                    connect(event);
                    break;
                }

                case NODE_REMOVED: {
                    logger.fine("Node removed: " + ZKPaths.getNodeFromPath(event.getData().getPath()));

                    String path = event.getData().getPath();

                    if (path.endsWith("kafkaUrl")) {
                        nodeMapping.get(path).setKafkaUrl(null);
                    } else if (path.endsWith("topic")) {
                        nodeMapping.get(path).setKafkaTopic(null);
                    } else {
                        logger.warning("Unexpected znode: " + path);
                        return;
                    }

                    if(!isValidConfig())
                    {
                        stopProcessor();
                    }
                    //Disconnect
                    stopProcessor();
                    break;
                }
            }
        };
        cache.getListenable().

                addListener(listener);

    }

    private void connect(TreeCacheEvent event) {
        String path = event.getData().getPath();
        if (nodeMapping.keySet().contains(path)) {


            if (path.endsWith("kafkaUrl")) {
                nodeMapping.get(path).setKafkaUrl(new String(event.getData().getData()));
            } else if (path.endsWith("topic")) {
                nodeMapping.get(path).setKafkaTopic(new String(event.getData().getData()));
            } else {
                logger.warning("Unexpected znode: " + path);
                return;
            }

            if (isValidConfig()) {
                startProcessor();
            } else {
                logger.info("Not connected to topic");
            }
        }
    }

    private boolean isValidConfig(){
        boolean configComplete = true;
        for (KafkaConfig config : nodeMapping.values()) {
            if (!config.isValid()) {
                configComplete = false;
            }
        }

        return configComplete;
    }

}
