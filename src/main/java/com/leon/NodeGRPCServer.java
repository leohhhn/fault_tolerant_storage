package com.leon;

import com.google.api.Logging;
import com.leon.gRPC.*;
import com.leon.helpers.Role;
import io.grpc.stub.StreamObserver;

public class NodeGRPCServer extends StorageServiceGrpc.StorageServiceImplBase {

    public final Node node;
    public final LoggingService logger;

    public NodeGRPCServer(Node node, LoggingService logger) {
        this.node = node;
        this.logger = logger;
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

                    logger.writeLocal(cr);
                    logger.replicateOnFollowers(cr);

                    node.put(key, value);
                    response = buildOKStatus(reqID);
                }
                case DELETE -> {
                    if (key.isBlank()) {
                        response = buildKeyOrValueNotProvidedStatus(reqID);
                        break;
                    } else if (!node.keyExists(key)) {
                        response = buildKeyNotFoundStatus(reqID);
                        break;
                    }

                    logger.writeLocal(cr);
                    logger.replicateOnFollowers(cr);

                    node.delete(key);
                    response = buildOKStatus(reqID);
                    // todo implement that OK status returns values provided by client from memorys
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
}



