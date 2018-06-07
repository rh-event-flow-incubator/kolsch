package com.redhat.streaming.zk.wrapper;

import com.redhat.streaming.zk.messaging.AbstractProcessor;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Logger;

/**
 *
 */
public class ZKSingleTopicWrapper implements Runnable {

    private static final Logger logger = Logger.getLogger(ZKSingleTopicWrapper.class.getName());

    //ZK Setup
    private String zkUrl;
    private String path;
    private String topicNode = "/topic";
    private String kafkaUrlNode = "/kafkaUrl";

    private KafkaConfig kafkaConfig;

    //Handle on the thread we start
    private AbstractProcessor processor;
    private Future f = null;

    public ZKSingleTopicWrapper(String zkUrl, String path, AbstractProcessor processor) {
        this.zkUrl = zkUrl;
        this.path = path;
        this.processor = processor;
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
            kafkaConfig = new KafkaConfig(cache.getCurrentData(path + kafkaUrlNode), cache.getCurrentData(path + topicNode));

            //Connect if topic not null, ie. node exists
            if (kafkaConfig.isValid()) {
                startProcessor(kafkaConfig);
            } else {
                logger.info("Not connected to topic");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Connect a thread which will consume a topic to the named topic. Will disconnect the existing thread if
     * it exists before connecting to a new one
     */
    private void startProcessor(KafkaConfig config) {

        try {

            String clazzName = processor.getClass().getCanonicalName();

            // Shut down existing processor if it exists
            stopProcessor();

            // Create the consumer thread
            final ExecutorService executor = Executors.newSingleThreadExecutor();

            Class clazz = Class.forName(clazzName);
            processor = (AbstractProcessor) clazz.newInstance();

            processor.init(config.getKafkaUrl(), config.getKafkaTopic());
            f = executor.submit(processor);

            logger.info("Started thread to connect to Topic: " + config.getKafkaTopic() + ". Obtained from ZK: " + path + topicNode);
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

                    logger.info("Node added: " + ZKPaths.getNodeFromPath(event.getData().getPath()));

                    //Connect
                    connect(event);
                    break;
                }

                case CHILD_UPDATED: {
                    logger.info("Node changed: " + ZKPaths.getNodeFromPath(event.getData().getPath()) + ". New value: " + new String(event.getData().getData()));

                    //Reconnect
                    connect(event);
                    break;
                }

                case CHILD_REMOVED: {
                    logger.info("Node removed: " + ZKPaths.getNodeFromPath(event.getData().getPath()));

                    //Disconnect
                    stopProcessor();
                    break;
                }
            }
        };
        cache.getListenable().addListener(listener);
    }

    private void connect(PathChildrenCacheEvent event) {
        if (event.getData().getPath().equals(path + topicNode)) {
            kafkaConfig.setKafkaTopic(new String(event.getData().getData()));
            if (kafkaConfig.isValid()) {
                startProcessor(kafkaConfig);
            }
        } else if (event.getData().getPath().equals(path + kafkaUrlNode)) {
            kafkaConfig.setKafkaUrl(new String(event.getData().getData()));
            if (kafkaConfig.isValid()) {
                startProcessor(kafkaConfig);
            }
        }
    }


}
