package com.leon;

import com.leon.gRPC.StorageServiceGrpc;

public class FollowerGRPCChannel {
    final String zkNode;
    final String connectionString;
    final StorageServiceGrpc.StorageServiceBlockingStub blockingStub;

    public FollowerGRPCChannel(String zkNode, String connectionString, StorageServiceGrpc.StorageServiceBlockingStub blockingStub){
        this.zkNode = zkNode;
        this.connectionString = connectionString;
        this.blockingStub = blockingStub;
    }

    public String getZkNode() {
        return zkNode;
    }

    public String getConnectionString() {
        return connectionString;
    }

    public StorageServiceGrpc.StorageServiceBlockingStub getBlockingStub() {
        return blockingStub;
    }

    @Override
    public String toString() {
        return "FollowerGRPCChannel{" +
                "zkNode='" + zkNode + '\'' +
                ", connectionString='" + connectionString + '\'' +
                '}';
    }
}
