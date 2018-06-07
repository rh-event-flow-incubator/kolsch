package com.redhat.streaming.zk.messaging;

import java.util.List;
import java.util.Map;

public abstract class AbstractProcessor implements Runnable{

    public void init(String kafkaUrl, String topic){}

    public void init(String kafkaUrl, List<String> nodeTopicMapping){}

    public abstract void shutdown();
}
