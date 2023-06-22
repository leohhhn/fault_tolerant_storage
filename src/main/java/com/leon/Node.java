package com.leon;

import io.grpc.Context;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Node implements Watcher, Runnable {
    private ZooKeeper zk;
    private final LoggingService logger;
    private final String nodeAddress;
    private boolean isLeader = false;
    private Map<String, String> storageMap;
    protected Integer mutex;

    volatile boolean running = false;
    private Thread thread = null;
    protected String rootZNode;

    Map<String, FollowerGRPCChannel> followersChannelMap = new HashMap<String, FollowerGRPCChannel>();

    public Node(String zookeeperAddress, String nodeAddress, String logFilePath) throws Exception {

        this.nodeAddress = nodeAddress;
        this.storageMap = new HashMap<String, String>();

        connectToZookeeper(zookeeperAddress);
        // todo check if logs exist and apply them
        // find last log index
        election();

        // if not leader wait for leader info, reply w wrong log index if youre missing logs


        // followers map should be instantiated after election
        this.logger = new LoggingService(logFilePath, followersChannelMap);

        int port = Integer.parseInt(nodeAddress.split(":")[1]);

        Server grpcServer = ServerBuilder
                .forPort(port)
                .addService(new NodeGRPCServer(this, logger))
                .build();

        grpcServer.start();

        try {


        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    private void connectToZookeeper(String address) {
        if (zk == null) {
            try {
                System.out.println("Connecting to ZK:");
                zk = new ZooKeeper(address, 3000, this);
                mutex = -1;
                System.out.println("Finished starting ZK: " + zk);
            } catch (IOException e) {
                e.printStackTrace();
                zk = null;
            }
        }
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

    public boolean keyExists(String key) {
        return this.storageMap.containsKey(key);
    }

    synchronized public void process(WatchedEvent event) {
        synchronized (mutex) {
            mutex.notify();
        }
    }

    public void start() {
        if (!running) {
            thread = new Thread(this, "Node");
            running = true;
            thread.start();
        }
    }

    public void stop() {
        Thread stopThread = thread;
        thread = null;
        running = false;
        stopThread.interrupt();
    }

    @Override
    public void run() {
        while (running) {
            synchronized (mutex) {
                try {

                    // TODO SEE IF YOU ACTUALLY NEED THREADS LOL

                    mutex.wait();
                    System.out.println("Desila se promena u konfiguraciji sistema");
                    election();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }


}
