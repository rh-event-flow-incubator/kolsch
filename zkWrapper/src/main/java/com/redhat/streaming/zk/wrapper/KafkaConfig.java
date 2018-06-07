package com.redhat.streaming.zk.wrapper;

import org.apache.curator.framework.recipes.cache.ChildData;

import java.util.UUID;

public class KafkaConfig {

    private String kafkaUrl;

    private String kafkaTopic;

    public KafkaConfig() {
    }

    public KafkaConfig(ChildData url, ChildData topic){

        if(url != null && url.getData() != null){
            this.kafkaUrl = new String(url.getData());
        }
        if(topic != null && topic.getData() != null){
            this.kafkaTopic = new String (topic.getData());
        }
    }

    public String getKafkaUrl() {
        return kafkaUrl;
    }

    public void setKafkaUrl(String kafkaUrl) {
        this.kafkaUrl = kafkaUrl;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public void setKafkaTopic(String kafkaTopic) {
        this.kafkaTopic = kafkaTopic;
    }

    public boolean isValid(){
        if(kafkaUrl == null || kafkaUrl.equals("")) return false;
        if(kafkaTopic == null || kafkaTopic.equals("")) return false;
        return true;
    }

    @Override
    public String toString() {
        return "KafkaConfig{" +
                "kafkaUrl='" + kafkaUrl + '\'' +
                ", kafkaTopic='" + kafkaTopic + '\'' +
                '}';
    }
}
