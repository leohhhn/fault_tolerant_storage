package com.leon;

import java.io.Serializable;
import java.util.Map;

public class Snapshot implements Serializable {
    public Map<String, String> storageMap;
    public int lastLogIndex;

    public Snapshot(Map<String, String> storageMap, int lastLogIndex) {
        this.storageMap = storageMap;
        this.lastLogIndex = lastLogIndex;
    }

    public Map<String, String> getStorageMap() {
        return storageMap;
    }

    public void setStorageMap(Map<String, String> storageMap) {
        this.storageMap = storageMap;
    }

    public int getLastLogIndex() {
        return lastLogIndex;
    }

    public void setLastLogIndex(int lastLogIndex) {
        this.lastLogIndex = lastLogIndex;
    }
}