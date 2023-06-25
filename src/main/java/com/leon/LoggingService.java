package com.leon;

import com.leon.gRPC.*;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LoggingService {
    private int lastLogIndex = 1; // first log is #1
    private final String logFilePath;
    private Map<String, FollowerGRPCChannel> followerChannelMap;

    private SnapshotService snapshotService;

    /**
     * Log entry examples:
     * Log #X: PUT:KEY:VALUE:UNIX_TIMESTAMP
     * Log #X: DELETE:KEY:VALUE:UNIX_TIMESTAMP
     */

    public LoggingService(String logFilePath, Map<String, FollowerGRPCChannel> followerGRPCChannelMap, SnapshotService snapshotService) {
        this.logFilePath = logFilePath;
        this.followerChannelMap = followerGRPCChannelMap;
        this.snapshotService = snapshotService;
    }

    /// Writes a CommandRequest cr to local log file
    /// Called when receiving CommandRequest from User
    public String writeLocal(CommandRequest cr) {
        try {
            String logEntry = createLogString(cr);

            System.out.println(logEntry);
            BufferedWriter writer = new BufferedWriter(new FileWriter(logFilePath, true));
            writer.write(logEntry);
            writer.newLine();  // Line separator

            writer.close();
            incrementLastLogIndex();
            return logEntry;
        } catch (IOException e) {
            System.out.println("An error occurred while writing the log.");
            e.printStackTrace();
        }
        return null;
    }

    public void writeLocalFromLog(String log) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(logFilePath, true));
            writer.write(log);
            writer.newLine();
            writer.close();
        } catch (IOException e) {
            System.out.println("An error occurred while writing the log.");
            e.printStackTrace();
        }
    }

    public void replicateOnFollowers(String log) throws InterruptedException {
        LogMessage logToReplicate = LogMessage.newBuilder()
                .setLog(log)
                .setLogIndex(lastLogIndex - 1)
                .build();

        LogResponse response;
        List<FollowerGRPCChannel> busyFollowers = new ArrayList<>();

        for (FollowerGRPCChannel follower : followerChannelMap.values()) {
            response = follower.blockingStub.appendLog(logToReplicate);
            if (response.getStatus() == LogStatus.FOLLOWER_BUSY) {
                // wait a little :)
                busyFollowers.add(follower);
            } else if (response.getStatus() == LogStatus.LOG_MISMATCH) {
                sendMissingLogs(follower, response.getLastEntryIndex());
            }
        }

        Thread.sleep(500);
        for (FollowerGRPCChannel follower : busyFollowers) {
            // try again
            response = follower.blockingStub.appendLog(logToReplicate);
            // todo implement retrys
        }
    }

    public void sendMissingLogs(FollowerGRPCChannel follower, int startIndex) {
        List<String> missingLogs = findLogsFrom(startIndex);
        LogMessage logToReplicate;

        for (int i = 0; i < missingLogs.size(); i++) {
            logToReplicate = LogMessage.newBuilder()
                    .setLog(missingLogs.get(i))
                    .setLogIndex(startIndex + i)
                    .build();

            follower.blockingStub.appendLog(logToReplicate);
            // currently ignores responses
        }
    }

    public void restoreState(Node node, String logFileName) {

        // see if there is snapshot
        // snapshots are done every 5 logs
        // if there is, ie up and including log#5 & last log is #8
        // apply snapshot to state, lastIndex = 5;
        // after applying snapshot to state

        Snapshot s = snapshotService.readSnapshot();

        if (s != null) {
            node.setStorageMap(s.getStorageMap());
            lastLogIndex = s.getLastLogIndex();
            System.out.println("Reading from local snapshot - last log is #" + lastLogIndex);
        }

        try {
            BufferedReader reader = new BufferedReader(new FileReader(logFileName));
            System.out.println("Log file found - restoring state");

            String line;
            while ((line = reader.readLine()) != null) {
                if (getIndexFromLog(line) > lastLogIndex) {
                    System.out.println("Applying log" + getIndexFromLog(line));
                    parseAndApplyLog(node, line);
                }
            }
            if (s == null)
                incrementLastLogIndex();

            reader.close();
            // can now start normal interaction

        } catch (FileNotFoundException e) {
            System.out.println("No log file found. Continuing normal operation");
        } catch (IOException e) {
            System.out.println("An error occurred while reading the log.");
            e.printStackTrace();
        }
        node.setNotBusy();
    }

    private void parseAndApplyLog(Node node, String log) {
        String[] parts = log.split(":");

        String commandType = parts[1].trim();
        String key = parts[2].trim();
        String value = parts[3].trim();

        if (commandType.equals("PUT")) {
            node.put(key, value);
            incrementLastLogIndex();
        } else if (commandType.equals("DELETE")) {
            node.delete(key);
            incrementLastLogIndex();
        }
    }

    public String createLogString(CommandRequest request) {
        StringBuilder sb = new StringBuilder();

        // Append the request ID
        sb.append("Log #").append(lastLogIndex).append(": ");

        CommandType type = request.getOpType();

        sb.append(type).append(":");
        sb.append(request.getKey()).append(":");
        sb.append(request.getValue());

        if (type == CommandType.DELETE)
            sb.append(System.currentTimeMillis() / 1000L);
        else
            sb.append(":").append(System.currentTimeMillis() / 1000L);

        return sb.toString();
    }

    public void applyReplicatedLog(Node n, String log) {
        parseAndApplyLog(n, log);
    }

    public int getLastLogIndex() {
        return lastLogIndex;
    }

    private void incrementLastLogIndex() {
        this.lastLogIndex++;
    }

    public void setFollowerChannelMap(Map<String, FollowerGRPCChannel> followerChannelMap) {
        this.followerChannelMap = followerChannelMap;
    }

    public List<String> findLogsFrom(int startIndex) {
        List<String> logs = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(logFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                int logIndex = getIndexFromLog(line);
                if (logIndex >= startIndex) {
                    logs.add(line);
                }
            }
        } catch (IOException e) {
            System.out.println("An error occurred while reading the log file: " + e.getMessage());
        }

        return logs;
    }

    private int getIndexFromLog(String log) {
        String[] parts = log.split(":");

        String id = parts[0].replaceAll("[^0-9]", "");
        return Integer.parseInt(id);
    }
}

