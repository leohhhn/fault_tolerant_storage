package com.leon;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Node implements Watcher {

    private final ZooKeeper zookeeper;
    private final LoggingService loggingService;
    private final String nodeAddress;
    private List<String> followerAddresses;
    private boolean isLeader = false;


    private Map<String, String> storageMap = new HashMap<String, String>();


    public Node(String zookeeperAddress, String nodeAddress) throws Exception {
        this.nodeAddress = nodeAddress;
        this.zookeeper = new ZooKeeper(zookeeperAddress, 3000, this);
        this.loggingService = new LoggingService();


        // Initialize the list of follower addresses
        this.followerAddresses = new ArrayList<>();

        // Node-specific setup tasks
        setup();
    }

    private void setup() throws Exception {
        // Node setup logic goes here. For example, connect to ZooKeeper, create Znode, etc.
    }

    public void process(WatchedEvent event) {
        // This method is called when something changes in ZooKeeper
    }

    private void election() {
        // Leader election logic goes here
    }

    public void put(String key, String value) {
        // assumes values are non-null and correct. in case of already existing key we do an update.
        this.storageMap.put(key, value);
    }

    public void delete(String key) {
        // assumes key:value pair exists and simply removes it from the map
        this.storageMap.remove(key);
    }

    public String read(String key) {
        return storageMap.get(key);
    }

    public void setIsLeader(boolean isLeader) {
        this.isLeader = isLeader;
    }

    public boolean getIsLeader() {
        return this.isLeader;
    }

    public boolean keyExists(String key){
        return this.storageMap.containsKey(key);
    }

}
