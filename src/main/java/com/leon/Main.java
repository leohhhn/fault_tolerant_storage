package com.leon;

public class Main {
    public static void main(String[] args) {


        // unsafe args, todo add security
        String zookeeperAddress;
        String port = null;
        String logFilePath = null;
        zookeeperAddress = args[0];

        if (args.length == 3) {
            port = args[1];
            logFilePath = args[2];
        }

        Node node;
        User u;

        try {

            if (args.length == 3)
                node = new Node(zookeeperAddress, port, logFilePath);
            else
                u = new User(zookeeperAddress);
        } catch (Exception e) {
            System.out.println("System crashed at boot! Stack trace:");
            e.printStackTrace();
        }
    }
}