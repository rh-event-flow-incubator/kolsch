package com.redhat.streaming.zk.wrapper;

import com.redhat.streaming.zk.messaging.AbstractProcessor;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Logger;

/**
 * todo: Update to take the URL of the Kafka server
 */
public class ZKMultiTopicWrapper implements Runnable {

    private static final Logger logger = Logger.getLogger(ZKMultiTopicWrapper.class.getName());

    //ZK Setup
    private String zkUrl;
    private String path;
    private List<String> nodes;
    private Map<String, String> nodeTopicMapping = new HashMap<>();

    //Kafka Setup
    private AbstractProcessor processor;
    private String kafkaUrl;
    private String consumerGroupId;

    //Handle on the thread we start
    private Future f = null;

    public ZKMultiTopicWrapper(String zkUrl, String path, List<String> nodes, AbstractProcessor processor, String kafkaUrl, String consumerGroupId) {
        this.zkUrl = zkUrl;
        this.path = path;
        this.nodes = nodes;
        this.processor = processor;
        this.kafkaUrl = kafkaUrl;
        this.consumerGroupId = consumerGroupId;
        for (String node : nodes) {
            nodeTopicMapping.put(path + node, null);
        }
    }

    @Override
    public void run() {

        CuratorFramework client;
        PathChildrenCache cache;

        try {
            //Zookeeper setup using Curator
            client = CuratorFrameworkFactory.newClient(zkUrl, new ExponentialBackoffRetry(1000, 3));
            client.start();

            // Use a Path cache to allow multiple nodes in future
            cache = new PathChildrenCache(client, path, true);
            cache.start();
            cache.rebuild();
            addListener(cache);

            //Initial stream setup if the node exists in ZK
            for (String nodePlusPath : nodeTopicMapping.keySet()) {

                ChildData data = cache.getCurrentData(nodePlusPath);
                if (data != null) {
                    nodeTopicMapping.put(nodePlusPath, new String(data.getData()));
                }
            }

            if (!nodeTopicMapping.containsValue(null)) {
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

            processor.init(kafkaUrl, consumerGroupId, path, nodes, nodeTopicMapping);
            f = executor.submit(processor);

            StringBuilder nodeList = new StringBuilder();
            for (String node : nodes) {
                nodeList.append(nodeTopicMapping.get(path + node)).append(", ");
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
    private void addListener(PathChildrenCache cache) {

        PathChildrenCacheListener listener = (client, event) -> {

            switch (event.getType()) {
                case CHILD_ADDED: {

                    logger.fine("Node added: " + ZKPaths.getNodeFromPath(event.getData().getPath()));

                    //Connect
                    String path = event.getData().getPath();
                    if (nodeTopicMapping.keySet().contains(path)) {
                        nodeTopicMapping.put(path, new String(event.getData().getData()));
                        if (!nodeTopicMapping.values().contains(null)) {
                            startProcessor();
                        }
                    }
                    break;
                }

                case CHILD_UPDATED: {
                    logger.fine("Node changed: " + ZKPaths.getNodeFromPath(event.getData().getPath()) + ". New value: " + new String(event.getData().getData()));

                    //Reconnect
                    String path = event.getData().getPath();
                    if (nodeTopicMapping.keySet().contains(path)) {
                        nodeTopicMapping.put(path, new String(event.getData().getData()));
                        if (!nodeTopicMapping.values().contains(null)) {
                            startProcessor();
                        }
                    }
                    break;
                }

                case CHILD_REMOVED: {
                    logger.fine("Node removed: " + ZKPaths.getNodeFromPath(event.getData().getPath()));

                    String path = event.getData().getPath();
                    if (nodeTopicMapping.keySet().contains(path)) {
                        nodeTopicMapping.put(path, new String(event.getData().getData()));

                        stopProcessor();
                    }
                    //Disconnect
                    stopProcessor();
                    break;
                }
            }
        };
        cache.getListenable().addListener(listener);
    }


}
