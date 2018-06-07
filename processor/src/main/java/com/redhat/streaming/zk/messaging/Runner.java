package com.redhat.streaming.zk.messaging;

import com.redhat.streaming.zk.wrapper.ZKMultiTopicWrapper;
import com.redhat.streaming.zk.wrapper.ZKSingleTopicWrapper;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Runner {

    private static final Logger logger = Logger.getLogger(Runner.class.getName());

    private static final String zkUrlVar = "ZK_URL";
    private static final String zkPathVar = "ZK_PATH";

    public static void main(String... args) {
        logger.info("Starting Consumer");

        final String zkUrl = resolve(zkUrlVar);
        final String zkPath = resolve(zkPathVar);

        final ExecutorService executor = Executors.newSingleThreadExecutor();
        List<String> nodes = Arrays.asList("/in", "/out");
        ZKMultiTopicWrapper zkProcessor = new ZKMultiTopicWrapper(zkUrl, zkPath, nodes, new SimpleProcessor()); //, "", "group2");
        executor.submit(zkProcessor);

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
