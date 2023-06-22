package com.leon;

public class Main {
    public static void main(String[] args) {


        if (args.length != 3) {
            System.out.println("Usage: java -cp <path_to_JAR> <zookeeper_address:zookeeper_port> <node_port> <log_file_name>");
            System.exit(1);
        }

        // todo add checks for args
        String zookeeperAddress = args[0];
        String port = args[1];
        String logFilePath = args[2];

        try {
            Node node = new Node(zookeeperAddress, port, logFilePath);
        } catch (Exception e) {
            System.out.println("System crashed at boot! Stack trace:");
            e.printStackTrace();
        }
    }
}