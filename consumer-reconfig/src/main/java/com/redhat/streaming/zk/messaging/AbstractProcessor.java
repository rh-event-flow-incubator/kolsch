package com.redhat.streaming.zk.messaging;

import java.util.List;
import java.util.Map;

public abstract class AbstractProcessor implements Runnable{

    protected String topic;

    public void init(String topic){
        this.topic = topic;
    }

    public void init(String kafkaUrl, String consumerGroupId, String topic){}

    public void init(String kafkaUrl, String consumerGroupId, String path, List<String> nodes, Map<String, String> nodeTopicMapping){}

    public abstract void shutdown();
}
