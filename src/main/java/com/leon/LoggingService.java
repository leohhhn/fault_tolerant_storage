package com.leon;

import com.leon.gRPC.CommandRequest;
import com.leon.gRPC.CommandType;

import java.io.*;
import java.util.Map;

public class LoggingService {


    private int lastLogIndex = 1; // fisrt log is #1
    private final String logFilePath;
    private Map<String, FollowerGRPCChannel> followerChannelMap;

    /**
     * Log entry examples:
     * Log #X: PUT:KEY:VALUE:UNIX_TIMESTAMP
     * Log #X: DELETE:KEY:VALUE:UNIX_TIMESTAMP
     */

    public LoggingService(String logFilePath, Map<String, FollowerGRPCChannel> followerGRPCChannelMap) {
        this.logFilePath = logFilePath;
        this.followerChannelMap = followerGRPCChannelMap;
    }

    public void writeLocal(CommandRequest cr) {
        // writes to local log file
        try {
            String logEntry = createLogString(cr);
            BufferedWriter writer = new BufferedWriter(new FileWriter(logFilePath, true));
            writer.write(logEntry);
            writer.newLine();  // Line separator
            // todo maybe flush?
            writer.close();
        } catch (IOException e) {
            System.out.println("An error occurred while writing the log.");
            e.printStackTrace();
        }
        lastLogIndex++;
    }


    public void replicateOnFollowers(CommandRequest cr) {
        // replicates on followers

    }


    public String createLogString(CommandRequest request) {
        StringBuilder sb = new StringBuilder();

        // Append the request ID
        sb.append("Log #").append(String.valueOf(lastLogIndex)).append(": ");

        CommandType type = CommandType.forNumber(request.getOpTypeValue());

        // Append the command type
        sb.append(type.toString()).append(":");
        // Append the key
        sb.append(request.getKey()).append(":");
        // Append the value
        sb.append(request.getValue());
        sb.append(":").append(System.currentTimeMillis() / 1000L);

        return sb.toString();
    }

    public void setFollowerChannelMap(Map<String, FollowerGRPCChannel> followerChannelMap) {
        this.followerChannelMap = followerChannelMap;
    }

    public void applyLogToState(Node node, String logFileName) {
        // deserialize log from local log file


        try {
            BufferedReader reader = new BufferedReader(new FileReader(logFileName));
            String line;
            while ((line = reader.readLine()) != null) {

                String[] parts = line.split(":");

                String commandType = parts[1].trim();
                String key = parts[2].trim();
                String value = parts[3].trim();

                if (commandType.equals("PUT")) {
                    node.put(key, value);
                    lastLogIndex++;
                } else if (commandType.equals("DELETE")) {
                    node.delete(key);
                    lastLogIndex++;
                }
                // third case data is corrupt or unreadable for some other reason
                // do nothing in that case
            }

            reader.close();
        } catch (FileNotFoundException e) {
            System.out.println("No log file found. Continuing normal operation");
        } catch (IOException e) {
            System.out.println("An error occurred while reading the log.");
            e.printStackTrace();
        }
    }

    public int getLastLogIndex() {
        return lastLogIndex;
    }

}
