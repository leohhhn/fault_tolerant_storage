package com.leon;

import com.leon.gRPC.CommandRequest;
import com.leon.gRPC.CommandType;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class LoggingService {


    private int lastLogIndex = -1;
    private final String logFilePath;
    private final Map<String, FollowerGRPCChannel> followerGRPCChannelMap;

    public LoggingService(String logFilePath, Map<String, FollowerGRPCChannel> followerGRPCChannelMap) {
        this.logFilePath = logFilePath;
        this.followerGRPCChannelMap = followerGRPCChannelMap;
    }

    public void writeLocal(CommandRequest cr) {
        // writes to local log file
        try {
            String logEntry = createLogString(cr);
            BufferedWriter writer = new BufferedWriter(new FileWriter(logFilePath, true));
            writer.write(logEntry);
            writer.newLine();  // Line separator
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
        sb.append(type.toString()).append(": ");
        // Append the key
        sb.append(request.getKey()).append(": ");
        // Append the value
        sb.append(request.getValue());
        sb.append(": ").append(System.currentTimeMillis() / 1000L);

        return sb.toString();
    }
}
