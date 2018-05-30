package com.redhat.streaming.zk.wrapper;

import com.redhat.streaming.zk.messaging.AbstractProcessor;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
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
    private String node;

    //Kafka Setup
    private AbstractProcessor processor;
    private String kafkaUrl;
    private String consumerGroupId;

    //Handle on the thread we start
    private Future f = null;

    public ZKSingleTopicWrapper(String zkUrl, String path, String node, AbstractProcessor processor, String kafkaUrl, String consumerGroupId) {
        this.zkUrl = zkUrl;
        this.path = path;
        this.node = node;
        this.processor = processor;
        this.kafkaUrl = kafkaUrl;
        this.consumerGroupId = consumerGroupId;
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
            ChildData data = cache.getCurrentData(path + node);

            //Connect if topic not null, ie. node exists
            if(data != null){
                startProcessor(new String(data.getData()));
            }else{
                logger.info("Not connected to topic");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Connect a thread which will consume a topic to the named topic. Will disconnect the existing thread if
     * it exists before connecting to a new one
     * @param topic name of the new topic to connect to
     */
    private void startProcessor(String topic){

        try {

            String clazzName = processor.getClass().getCanonicalName();

            // Shut down existing processor if it exists
            stopProcessor();

            // Create the consumer thread
            final ExecutorService executor = Executors.newSingleThreadExecutor();

            Class clazz = Class.forName(clazzName);
            processor = (AbstractProcessor) clazz.newInstance();

            processor.init(kafkaUrl, consumerGroupId, topic);
            f = executor.submit(processor);

            logger.info("Started thread to connect to Topic: " + topic + ". Obtained from ZK: " + path + node);
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            e.printStackTrace();
        }
    }

    /**
     * Terminate the thread
     */
    private void stopProcessor(){

            if (f != null) {
                logger.info("Consumer disconnecting");
                processor.shutdown();
                //todo: check that the consumer thread dies
        }
    }

    /**
     * Add a listener to get updates from the cache for the given children of the path.
     * @param cache Cache to get notifications from
     */
    private void addListener(PathChildrenCache cache) {

        PathChildrenCacheListener listener = (client, event) -> {

            switch (event.getType()) {
                case CHILD_ADDED: {

                    logger.fine("Node added: " + ZKPaths.getNodeFromPath(event.getData().getPath()));

                    //Connect
                    if(event.getData().getPath().equals(path + node)){
                        startProcessor(new String(event.getData().getData()));
                    }
                    break;
                }

                case CHILD_UPDATED: {
                    logger.fine("Node changed: " + ZKPaths.getNodeFromPath(event.getData().getPath())  + ". New value: " + new String(event.getData().getData()));

                    //Reconnect
                    if(event.getData().getPath().equals(path  + node)){
                        startProcessor(new String(event.getData().getData()));
                    }
                    break;
                }

                case CHILD_REMOVED: {
                    logger.fine("Node removed: " + ZKPaths.getNodeFromPath(event.getData().getPath()));

                    //Disconnect
                    stopProcessor();
                    break;
                }
            }
        };
        cache.getListenable().addListener(listener);
    }


}
