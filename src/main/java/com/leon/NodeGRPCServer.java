package com.leon;

import com.google.api.Logging;
import com.leon.gRPC.*;
import com.leon.helpers.Role;
import io.grpc.stub.StreamObserver;
import org.apache.zookeeper.server.admin.Command;

public class NodeGRPCServer extends StorageServiceGrpc.StorageServiceImplBase {

    public final Node node;
    public final LoggingService logger;

    private final int SNAPSHOT_SCHEDULE_LOGS = 15;
    private int logsUntilSnapshot = 15;

    private SnapshotService snapshotService;

    public NodeGRPCServer(Node node, LoggingService logger, SnapshotService snapshotService) {
        this.node = node;
        this.logger = logger;
        this.snapshotService = snapshotService;
    }

    public void command(CommandRequest cr, StreamObserver<CommandResponse> responseObserver) {
        CommandResponse response;
        CommandType type = CommandType.forNumber(cr.getOpTypeValue());

        if (type == null) return; // todo throw error

        String key = cr.getKey();
        String value = cr.getValue();
        Integer reqID = cr.getRequestId();

        if (type != CommandType.READ && node.getNodeRole() != Role.LEADER) {
            response = buildRejectedNotLeaderStatus(reqID);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }

        try {
            switch (type) {
                case PUT -> {
                    if (!checkPutValues(key, value)) {
                        response = buildKeyOrValueNotProvidedStatus(reqID);
                        break;
                    }

                    String log = logger.writeLocal(cr); // log logic should contain write and increase log index;
                    logger.replicateOnFollowers(log);

                    node.put(key, value);
                    response = buildOKStatus(reqID);
                    --logsUntilSnapshot;
                }
                case DELETE -> {
                    if (key.isBlank()) {
                        response = buildKeyOrValueNotProvidedStatus(reqID);
                        break;
                    } else if (!node.keyExists(key)) {
                        response = buildKeyNotFoundStatus(reqID);
                        break;
                    }

                    String log = logger.writeLocal(cr);
                    logger.replicateOnFollowers(log);

                    node.delete(key);
                    response = buildOKStatus(reqID);
                    --logsUntilSnapshot;
                    // todo implement that OK status returns values provided by client from memory
                }

                case READ -> {
                    // READ request ignores value field from user.
                    // todo make this more strict, for security reasons

                    if (node.getNodeRole() != Role.LEADER && !isSyncedWithLeader()) {
                        response = buildNotSynced(reqID);
                        break;
                    }

                    if (key.isBlank()) {
                        response = buildKeyOrValueNotProvidedStatus(reqID);
                        break;
                    } else if (!node.keyExists(key)) {
                        response = buildKeyNotFoundStatus(reqID);
                        break;
                    }

                    String valueFromMap = node.read(key);
                    response = CommandResponse.newBuilder()
                            .setRequestId(reqID)
                            .setStatus(RequestStatus.STATUS_OK)
                            .setKey(key)
                            .setValue(valueFromMap)
                            .build();
                }
                default -> {
                    response = buildUnrecognizedStatus(reqID);
                }
            }
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            e.printStackTrace();
        }

        if (logsUntilSnapshot == 0) {
            snapshotService.makeSnapshot(logger.getLastLogIndex()-1);
            logsUntilSnapshot = SNAPSHOT_SCHEDULE_LOGS;
        }
    }

    public void appendLog(LogMessage l, StreamObserver<LogResponse> responseObserver) {
        // get log in as string format defined in logging service
        // check if local last log index+1 == what is being sent
        // apply log locally

        int lastLogIndex = logger.getLastLogIndex();

        LogResponse response;
        String log = l.getLog();
        int logIndex = l.getLogIndex();

        if (node.busy()) {
            response = buildFollowerBusy();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            System.out.println("Leader tried replicating but I'm busy");
            return;
        }

        if (logIndex != lastLogIndex) {
            response = buildLogMismatch(lastLogIndex);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            System.out.println("LOG MISMATCH: Last local log is #" + lastLogIndex);
            return;
        }

        System.out.println(log);

        logger.applyReplicatedLog(node, log);
        logger.writeLocalFromLog(log);

        response = buildLogOK(logIndex); // returns the index of log that was just written to sender

        responseObserver.onNext(response);
        responseObserver.onCompleted();

        --logsUntilSnapshot;

        if (logsUntilSnapshot == 0) {
            snapshotService.makeSnapshot(logger.getLastLogIndex()-1);
            logsUntilSnapshot = SNAPSHOT_SCHEDULE_LOGS;
        }
    }


    private boolean checkPutValues(String key, String value) {
        // both key and value must not be "". return true if values are correct
        return !key.isBlank() && !value.isBlank();
    }

    private boolean isSyncedWithLeader() {
        // ask leader for last log
        // check your log
        // log indexes match => synced
        return false;
    }

    private CommandResponse buildOKStatus(Integer reqID) {
        return CommandResponse.newBuilder().
                setRequestId(reqID).
                setStatus(RequestStatus.STATUS_OK).
                build();
    }

    private CommandResponse buildRejectedNotLeaderStatus(Integer reqID) {
        String leaderGRPCAddress = node.getLeaderGRPCAddress(); // might be empty
        return CommandResponse.newBuilder()
                .setRequestId(reqID)
                .setStatus(RequestStatus.REJECTED_NOT_LEADER)
                .setKey("Current leader GRPC Address:")
                .setValue(leaderGRPCAddress)
                .build();
    }

    private CommandResponse buildKeyNotFoundStatus(Integer reqID) {
        return CommandResponse.newBuilder().
                setRequestId(reqID).
                setStatus(RequestStatus.KEY_NOT_FOUND).
                build();
    }

    private CommandResponse buildNotSynced(Integer reqID) {
        return CommandResponse.newBuilder().
                setRequestId(reqID).
                setStatus(RequestStatus.NOT_SYNCED).
                build();
    }

    private CommandResponse buildKeyOrValueNotProvidedStatus(Integer reqID) {
        return CommandResponse.newBuilder().
                setRequestId(reqID).
                setStatus(RequestStatus.KEY_OR_VALUE_NOT_PROVIDED).
                build();
    }

    private CommandResponse buildUnrecognizedStatus(Integer reqID) {
        return CommandResponse.newBuilder().
                setRequestId(reqID).
                setStatus(RequestStatus.UNRECOGNIZED).
                build();
    }

    private LogResponse buildLogMismatch(int lastEntryIndex) {
        return LogResponse.newBuilder()
                .setLastEntryIndex(lastEntryIndex)
                .setStatus(LogStatus.LOG_MISMATCH)
                .build();
    }

    private LogResponse buildFollowerBusy() {
        return LogResponse.newBuilder()
                .setStatus(LogStatus.FOLLOWER_BUSY)
                .build();
    }

    private LogResponse buildLogOK(int lastEntryIndex) {
        return LogResponse.newBuilder()
                .setLastEntryIndex(lastEntryIndex)
                .setStatus(LogStatus.LOG_OK)
                .build();
    }

}



