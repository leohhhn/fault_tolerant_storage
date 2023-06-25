package com.leon;

public class Main {
    public static void main(String[] args) {

        // unsafe args, todo add security
        Node node;
        User u;

        String zookeeperAddress;
        String port = null;
        String logFilePath = null;
        String snapshotFilePath = null;

        zookeeperAddress = args[0];

        if (args.length == 4) {
            port = args[1];
            snapshotFilePath = args[2];
            logFilePath = args[3];
        }

        try {
            if (args.length == 4)
                node = new Node(zookeeperAddress, port, snapshotFilePath, logFilePath);
            else
                u = new User(zookeeperAddress);
        } catch (Exception e) {
            System.out.println("System crashed at boot! Stack trace:");
            e.printStackTrace();
        }
    }
}