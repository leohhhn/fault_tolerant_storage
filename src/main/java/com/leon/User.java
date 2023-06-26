package com.leon;

import com.leon.gRPC.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
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
    private boolean ready = false;
    Random r = new Random();

    public User(String zookeeperHosts) {
        try {
            connectToZookeeper(zookeeperHosts);
            findLeaderNode();
        } catch (Exception e) {
            System.out.println("Error with leader node!");
            findLeaderNode();
        }
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

    private synchronized void findLeaderNode() {
        try {
            Thread.sleep(2100);

            List<String> children = zk.getChildren(rootZNode, false);

            boolean leaderExists = false;
            for (String s : children) {
                if (s.equals("leader")) {
                    leaderExists = true;
                    children.remove(s);
                    break;
                }
            }

            if (!leaderExists) {
                waitForLeaderToJoin();
            }

            Collections.sort(children);

            leaderNodePath = children.get(0);

            System.out.println("GETTING LEADER ADDRESS:");
            this.leaderGRPCAddress = new String(zk.getData(rootZNode + "/" + leaderNodePath, false, null));
            System.out.println(this.leaderGRPCAddress);
            this.blockingStub = getBlockingStub(this.leaderGRPCAddress);
            interactWithServer();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void interactWithUser() {
        Scanner scanner = new Scanner(System.in);

        while (true) {
            if (!ready) {
                System.out.println("not ready!");
                continue;
            }

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
                    .setRequestId(abs(r.nextInt()))
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

    private void interactWithServer() {
        try {
            for (int i = 0; i < 50; i++) {
                sendRequests();
            }
            Thread.sleep(2000);
        } catch (Exception e) {
            System.out.println("Error with leader node!");
            e.printStackTrace();
            findLeaderNode();
        }
    }

    private void sendRequests() {
        // sends 4 requests to server
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

            Thread.sleep(500);
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

    public void waitForLeaderToJoin() throws InterruptedException {
        while (true) {
            // main node loop
            synchronized (zkSyncMutex) {
                try {
                    zkSyncMutex.wait();
                    System.out.println("System config changed! Re-electing.");
                    // rerun election
                    findLeaderNode();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
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
