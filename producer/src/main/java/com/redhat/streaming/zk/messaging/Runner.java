package com.redhat.streaming.zk.messaging;

import com.redhat.streaming.zk.wrapper.ZKSingleTopicWrapper;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Runner {

    private static final Logger logger = Logger.getLogger(Runner.class.getName());

    private static final String zkUrlVar = "ZK_URL"; //"localhost:2181";
    private static final String zkPathVar = "ZK_PATH"; // "/streams/app1";

    public static void main(String... args) {
        logger.info("Starting Demo");

        final String zkUrl = resolve(zkUrlVar);
        final String zkPath = resolve(zkPathVar);

        System.out.println("zkUrl = " + zkUrl);
        System.out.println("zkPath = " + zkPath);

        final ExecutorService executor = Executors.newSingleThreadExecutor();
        ZKSingleTopicWrapper zkProducer = new ZKSingleTopicWrapper(zkUrl, zkPath, new SimpleProducer());
        executor.submit(zkProducer);


        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            executor.shutdown();
            try {
                executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ie) {
                logger.log(Level.SEVERE, "Error on close", ie);
            }
        }));
    }

    private static String resolve(final String variable) {

        String value = System.getProperty(variable);
        if (value == null) {
            // than we try ENV ...
            value = System.getenv(variable);
        }
        return value;
    }

}
