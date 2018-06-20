package io.streamzi.zkMicroProfileConfig.rest;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Holder object for a set of parameters that are going to be maintained in ZooKeeper
 */
public class KolschConfig {

    //Base node of the paramters in ZK
    private String applicationId;

    //Mapping from key : value
    private Map<String, String> rows = new HashMap<>();

    public KolschConfig() {
    }

    public String getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    public Map<String, String> getRows() {
        return rows;
    }

    public void setRows(Map<String, String> rows) {
        this.rows = rows;
    }


    public String get(Object key) {
        return rows.get(key);
    }

    public String put(String key, String value) {
        return rows.put(key, value);
    }

    public String remove(Object key) {
        return rows.remove(key);
    }

    public Set<String> keySet() {
        return rows.keySet();
    }

    public Collection<String> values() {
        return rows.values();
    }

    @Override
    public String toString() {
        return "KolschConfig{" +
                "applicationId='" + applicationId + '\'' +
                ", rows=" + rows +
                '}';
    }


}
