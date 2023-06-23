package com.leon;

import com.leon.gRPC.*;
import com.sun.nio.sctp.PeerAddressChangeNotification;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;


import java.io.IOException;
import java.sql.SQLOutput;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static java.lang.Math.abs;

public class User implements Watcher {
    private ZooKeeper zk;
    private String leaderNodePath;
    private String leaderGRPCAddress;
    private String rootZNode = "/root";

    private Integer zkSyncMutex;

    private ManagedChannel leaderChannel = null;
    private StorageServiceGrpc.StorageServiceBlockingStub blockingStub = null;
    Random r = new Random();

    public User(String zookeeperHosts) {
        connectToZookeeper(zookeeperHosts);
        findLeaderNode();
        sendRequests();
    }

    private void connectToZookeeper(String address) {
        if (zk == null) {
            try {
                System.out.println("Connecting to ZK...");
                zk = new ZooKeeper(address, 3000, this);
                zkSyncMutex = -1;
            } catch (IOException e) {
                e.printStackTrace();
                zk = null;
            }
        }
    }

    private void findLeaderNode() {
        try {
            // Assuming the root node is / and children nodes represent servers
            List<String> children = zk.getChildren(rootZNode, false);

            Collections.sort(children);
            leaderNodePath = children.get(0);

            System.out.println("GETTING LEADER ADDRESS:");
            this.leaderGRPCAddress = new String(zk.getData(rootZNode + "/" + leaderNodePath, false, null));
            System.out.println(this.leaderGRPCAddress);
            this.blockingStub = getBlockingStub(this.leaderGRPCAddress);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void sendRequests() {
        CommandType type;

        type = CommandType.PUT;

        String key = "LAV";
        String value = "HUDAK";
        buildAndSendRequest(type, abs(r.nextInt()), key, value);

        type = CommandType.READ;
        buildAndSendRequest(type, abs(r.nextInt()), key, "");

        type = CommandType.DELETE;
        buildAndSendRequest(type, abs(r.nextInt()), key, "");

        type = CommandType.READ;
        buildAndSendRequest(type, abs(r.nextInt()), key, "");

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void buildAndSendRequest(CommandType type, int reqID, String key, String value) {
        CommandRequest request;
        CommandResponse response;
        request = CommandRequest.newBuilder()
                .setOpType(type)
                .setRequestId(reqID)
                .setKey(key)
                .setValue(value)
                .build();

        System.out.println("========= REQUEST =========");
        System.out.println("Type: " + type);
        System.out.println("Request ID: " + reqID);
        System.out.println("Key: " + key);
        System.out.println("Value: " + value);

        synchronized (this) {
            response = blockingStub.command(request);
        }
        System.out.println("========= RESPONSE =========");
        System.out.println("Request ID: " + response.getRequestId());
        System.out.println("Status: " + response.getStatus());
        System.out.println("Key: " + response.getKey());
        System.out.println("Value: " + response.getValue());

    }

    synchronized public void process(WatchedEvent event) {
        synchronized (zkSyncMutex) {
            zkSyncMutex.notify();
        }
    }


    public StorageServiceGrpc.StorageServiceBlockingStub getBlockingStub(String address) {
        String ip = address.split(":")[0];
        int port = Integer.parseInt(address.split(":")[1]);

        this.leaderChannel = ManagedChannelBuilder.forAddress(ip, port)
                .usePlaintext()
                .build();

        return StorageServiceGrpc.newBlockingStub(leaderChannel);
    }
}
