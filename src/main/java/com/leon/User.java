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
import java.util.*;

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
       // sendRequests();
        interactWithUser();
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

    public void interactWithUser() {
        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.println("============================");
            System.out.println("Enter your command (PUT/READ/DELETE), key and value (if needed):");
            String command = scanner.nextLine();

            String[] parts = command.split(" ");
            if (parts.length == 1) {
                System.out.println("Invalid command format. Should be: PUT KEY VALUE");
                continue;
            }

            if (parts[0].equalsIgnoreCase("PUT") && parts.length != 3) {
                System.out.println("Invalid command format. Should be: PUT KEY VALUE");
                continue;
            }

            if ((parts[0].equalsIgnoreCase("READ") || parts[0].equalsIgnoreCase("DELETE")) && parts.length != 2) {
                System.out.println("Invalid command format. Should be: " + parts[0].toUpperCase() + " KEY");
                continue;
            }

            CommandRequest.Builder requestBuilder = CommandRequest.newBuilder()
                    .setRequestId(r.nextInt())
                    .setKey(parts[1]);

            if (parts[0].equalsIgnoreCase("PUT")) {
                requestBuilder.setValue(parts[2]);
            }

            if (parts[0].equalsIgnoreCase("PUT")) {
                requestBuilder.setOpType(CommandType.PUT);
            } else if (parts[0].equalsIgnoreCase("DELETE")) {
                requestBuilder.setOpType(CommandType.DELETE);
            } else if (parts[0].equalsIgnoreCase("READ")) {
                requestBuilder.setOpType(CommandType.READ);
            } else {
                System.out.println("Invalid command type. Only PUT, READ or DELETE are allowed.");
                continue;
            }

            CommandRequest request = requestBuilder.build();
            CommandResponse response;

            printRequest(request);
            synchronized (this) {
                response = blockingStub.command(request);
            }
            printResponse(response);
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

        printRequest(request);
        synchronized (this) {
            response = blockingStub.command(request);
        }
        printResponse(response);
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

    private void printRequest(CommandRequest request) {
        System.out.println("========= REQUEST =========");
        System.out.println("Type: " + request.getOpType());
        System.out.println("Request ID: " + request.getRequestId());
        System.out.println("Key: " + request.getKey());
        System.out.println("Value: " + request.getValue());
    }

    private void printResponse(CommandResponse response) {
        System.out.println("========= RESPONSE =========");
        System.out.println("Request ID: " + response.getRequestId());
        System.out.println("Status: " + response.getStatus());
        System.out.println("Key: " + response.getKey());
        System.out.println("Value: " + response.getValue());
    }
}
