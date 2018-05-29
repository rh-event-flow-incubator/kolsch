package com.redhat.streaming.zk.demo;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 
 */
public class ZKConsumerWrapper implements Runnable {
    private static final Logger logger = Logger.getLogger(ZKConsumerWrapper.class.getName());

    private String zk_url = "localhost:2181";
    private String path = "/streams/app1";
    private String node = "/s1"; //might be interested in multiple in the future
    private SimpleConsumer consumer; //Message consumer that's going to do the work
    private Future f = null; //Handle to be able to terminate existing consumer

    //Kafka setup
    private String kafkaUrl = "localhost:9092";
    private String consumerGroupId = "group1";


    @Override
    public void run() {

        CuratorFramework client;
        PathChildrenCache cache;

        try {
            //Zookeeper setup using Curator
            client = CuratorFrameworkFactory.newClient(zk_url, new ExponentialBackoffRetry(1000, 3));
            client.start();

            // Use a Path cache to allow multiple nodes in future
            cache = new PathChildrenCache(client, path, true);
            cache.start();
            cache.rebuild();
            addListener(cache);

            //Initial stream setup if the node exists in ZK
            //todo: can we use cache.getPath(...)
            String topic = null;
            List<ChildData> children = cache.getCurrentData();
            for (ChildData child : children) {
                if (child.getPath().equals(path + node)) {
                    topic = new String(child.getData());
                }
            }

            //Connect if topic not null, ie. node exists
            if(topic != null){
                startConsumer(topic);
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
    private void startConsumer(String topic){

        // Shut down existing consumer if it exists
        stopConsumer();

        // Create the consumer thread
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        consumer = new SimpleConsumer(kafkaUrl, consumerGroupId, topic);
        f = executor.submit(consumer);

        logger.info("Started thread to connect to Topic: " + topic + ". Obtained from ZK: " + path + node);
    }

    /**
     * Terminate the consumer thread
     */
    private void stopConsumer(){

        if (consumer != null && consumer.isRunning()) {
            if (f != null) {
                logger.info("Consumer disconnecting");
                consumer.shutdown();
                //todo: check that the consumer thread dies
            }
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
                        startConsumer(new String(event.getData().getData()));
                    }
                    break;
                }

                case CHILD_UPDATED: {
                    logger.fine("Node changed: " + ZKPaths.getNodeFromPath(event.getData().getPath())  + ". New value: " + new String(event.getData().getData()));

                    //Reconnect
                    if(event.getData().getPath().equals(path  + node)){
                      startConsumer(new String(event.getData().getData()));
                    }
                    break;
                }

                case CHILD_REMOVED: {
                    logger.fine("Node removed: " + ZKPaths.getNodeFromPath(event.getData().getPath()));

                    //Disconnect
                    stopConsumer();
                    break;
                }
            }
        };
        cache.getListenable().addListener(listener);
    }

    public static void main(String... args) {

        logger.info("Starting ZK Wrapper");
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        ZKConsumerWrapper zkConsumerWrapper= new ZKConsumerWrapper();
        executor.submit(zkConsumerWrapper);


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
