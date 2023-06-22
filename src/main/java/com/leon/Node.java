import com.leon.LoggingService;
import com.leon.StorageService;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.util.ArrayList;
import java.util.List;

public class Node implements Watcher {

    private final ZooKeeper zookeeper;
    private final StorageService storageService;
    private final LoggingService loggingService;
    private final String nodeAddress;
    private List<String> followerAddresses;

    public Node(String zookeeperAddress, String nodeAddress) throws Exception {
        this.nodeAddress = nodeAddress;
        this.zookeeper = new ZooKeeper(zookeeperAddress, 3000, this);
        this.storageService = new StorageService();
        this.loggingService = new LoggingService();

        // Initialize the list of follower addresses
        this.followerAddresses = new ArrayList<>();

        // Node-specific setup tasks
        setup();
    }

    private void setup() throws Exception {
        // Node setup logic goes here. For example, connect to ZooKeeper, create Znode, etc.
    }

    public void process(WatchedEvent event) {
        // This method is called when something changes in ZooKeeper
    }

    private void election() {
        // Leader election logic goes here
    }

    public void put(String key, String value) {
        // Put logic goes here, including updating the HashMap, writing to the log,
        // and forwarding the put request to followers
    }

    public void delete(String key) {
        // Delete logic goes here, including updating the HashMap, writing to the log,
        // and forwarding the delete request to followers
    }

    // Other methods as needed...
}
