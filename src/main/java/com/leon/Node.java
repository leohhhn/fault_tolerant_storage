package com.leon;

import com.leon.gRPC.StorageServiceGrpc;
import com.leon.helpers.Role;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.*;

public class Node implements Watcher {
    private ZooKeeper zk = null;
    private LoggingService logger = null;
    private String port = "";
    private Map<String, String> storageMap = null;

    private Integer zkSyncMutex;
    private String rootZNode = "/root";
    private String nodeNamePrefix = "/node";
    private int lastLogEntry = -1;
    private String grpcAddress; // ip + port

    private int nodeID = -1;
    private volatile Role nodeRole = null;

    Map<String, FollowerGRPCChannel> followersChannelMap = null;
    private String leaderZNodePath = "";
    private String leaderGRPCAddress = "";


    public Node(String zookeeperAddress, String port, String logFilePath) throws Exception {

        this.port = port;
        this.storageMap = new HashMap<>();
        this.logger = new LoggingService(logFilePath, null);

        this.grpcAddress = "localhost" + ":" + port;

        logger.applyLogToState(this, logFilePath);

        lastLogEntry = logger.getLastLogIndex();

        connectToZookeeper(zookeeperAddress);
        joinZoo();

        try {
            election();
        } catch (Exception e) {
            System.out.println("Election failed!");
            e.printStackTrace();
        }

        // if not leader wait for leader info, reply w wrong log index if you're missing logs
        // followers map should be instantiated after election

        if (this.nodeRole == Role.LEADER)
            logger.setFollowerChannelMap(followersChannelMap);

        Server grpcServer = ServerBuilder.forPort(Integer.parseInt(port)).addService(new NodeGRPCServer(this, logger)).build();

        try {
            grpcServer.start();
            this.run(); // start listening
            grpcServer.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
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

    private void joinZoo() {
        if (zk != null) {
            try {
                System.out.println("Joining system...");
                Stat s = zk.exists(rootZNode, false);
                if (s == null) {
                    zk.create(rootZNode, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }

                String zNodePath = zk.create(rootZNode + nodeNamePrefix, grpcAddress.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                // example nodeName: /root/node0000000007

                System.out.println("Successfully joined system!");
                System.out.println("zNode path: " + zNodePath); // i.e /root/node0000000007

                this.nodeID = extractIDFromNodeName(zNodePath);
                System.out.println("Node ID: " + this.nodeID);

            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void election() throws Exception {
        List<String> list = zk.getChildren(rootZNode, true);
        int numOfNodes = list.size();

        System.out.println("========== ELECTION ===========");

        if (numOfNodes == 0)
            throw new Exception("0 nodes in system - is Zookeeper up?");

        printNodeList(list);

        // leader is the node with the smallest ID value in the system
        // ID values only keep rising due ZK's SEQUENTIAL mode

        int minID = Integer.MAX_VALUE;
        for (int i = 0; i < numOfNodes; i++) {
            int id = extractIDFromNodeName(list.get(i));
            if (id < minID) {
                minID = id;
                this.leaderZNodePath = rootZNode + '/' + list.get(i);
            }
        }

        if (minID == this.nodeID) {
            System.out.println("New leader ID: " + this.nodeID);
            setLeader(list);
        } else {
            System.out.println("Leader selected! Leader node ID: " + extractIDFromNodeName(this.leaderZNodePath));
            byte[] b = zk.getData(this.leaderZNodePath, false, null);
            this.leaderGRPCAddress = new String(b);
            setNodeRole(Role.FOLLOWER);
        }

        System.out.println("======== ELECTION OVER ========");

    }

    private void setLeader(List<String> list) throws InterruptedException, KeeperException {
        setNodeRole(Role.LEADER);
        createFollowerChannelMap(list);
        zk.getChildren(rootZNode, true);
    }

    private void createFollowerChannelMap(List<String> list) {
        Map<String, FollowerGRPCChannel> oldMap = followersChannelMap;
        followersChannelMap = new HashMap<>();

        for (String nodeName : list) {
            if (extractIDFromNodeName(nodeName) == this.nodeID)
                continue;

            FollowerGRPCChannel followerChannel = oldMap.get(nodeName);

            try {
                if (followerChannel == null) {
                    byte[] b = zk.getData(rootZNode + '/' + nodeName, false, null);
                    String grpcConnection = new String(b);
                    String[] tokens = grpcConnection.split(":");
                    ManagedChannel channel = ManagedChannelBuilder
                            .forAddress(tokens[0], Integer.parseInt(tokens[1]))
                            .usePlaintext()
                            .build();

                    StorageServiceGrpc.StorageServiceBlockingStub blockingStub = StorageServiceGrpc.newBlockingStub(channel);
                    followerChannel = new FollowerGRPCChannel(nodeName, grpcConnection, blockingStub);
                    System.out.println(followerChannel);
                } else {
                    oldMap.remove(nodeName);
                }
                followersChannelMap.put(nodeName, followerChannel);
            } catch (InterruptedException | KeeperException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void run() {
        while (true) {
            // main node loop
            synchronized (zkSyncMutex) {
                try {
                    zkSyncMutex.wait();
                    System.out.println("System config changed! Re-electing.");
                    // rerun election
                    election();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    synchronized public void process(WatchedEvent event) {
        synchronized (zkSyncMutex) {
            zkSyncMutex.notify();
        }
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

    public void setLastLogEntry(int n) {
        this.lastLogEntry = n;
    }

    public boolean keyExists(String key) {
        return this.storageMap.containsKey(key);
    }

    public int extractIDFromNodeName(String nodename) {
        String id = nodename.replaceAll("[^0-9]", "");
        return Integer.parseInt(id);
    }

    private void printNodeList(List<String> list) {
        if (list.size() == 1) {
            System.out.println("There is " + list.size() + " node in the system currently.");
        } else {
            System.out.println("There are a total of " + list.size() + " nodes in the system currently.");
            System.out.println("IDs of nodes in the system:");
            for (String s : list) System.out.print(extractIDFromNodeName(s) + " ");
            System.out.println();
        }
    }

    public Role getNodeRole() {
        return nodeRole;
    }

    public void setNodeRole(Role nodeRole) {
        this.nodeRole = nodeRole;
    }

    public String getLeaderGRPCAddress() {
        return leaderGRPCAddress;
    }

    public void setLeaderGRPCAddress(String leaderGRPCAddress) {
        this.leaderGRPCAddress = leaderGRPCAddress;
    }
}
