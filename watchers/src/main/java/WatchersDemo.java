import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;

public class WatchersDemo implements Watcher {
    // ZOOKEEPER ACTIONS TO TEST THIS
    // create /target "some data"
    // set /target "new data"
    // create /target/child
    // create /target/child2
    // delete /target/child2

    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private static final String TARGET_ZNODE = "/target";

    private ZooKeeper zooKeeper;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        WatchersDemo watchersDemo = new WatchersDemo();
        watchersDemo.connectToZookeeper();
        watchersDemo.watchTargetZnode();
        watchersDemo.run();
        System.out.println("Disconnected from Zookeeper, exiting application");
        watchersDemo.close();
    }

    public void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);

    }

    public void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        switch (watchedEvent.getType()) {
            case None:
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("Successfully connected to Zookeeper");
                } else {
                    synchronized (zooKeeper) {
                        System.out.println("Disconnected from Zookeeper event");
                        zooKeeper.notifyAll();
                    }
                }
                break;
            case NodeCreated:
                System.out.println("Node created event: " + TARGET_ZNODE);
                break;
            case NodeDeleted:
                System.out.println("Node deleted event: " + TARGET_ZNODE);
                break;
            case NodeDataChanged:
                System.out.println("Data changed event: " + TARGET_ZNODE);
                break;
            case NodeChildrenChanged:
                System.out.println("Children changed event: " + TARGET_ZNODE);
                break;
        }
        try {
            watchTargetZnode();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        }
    }

    public void watchTargetZnode() throws InterruptedException, KeeperException {
        Stat stat = zooKeeper.exists(TARGET_ZNODE, this);
        if (stat == null) {
            return;
        }

        byte[] data = zooKeeper.getData(TARGET_ZNODE, this, stat);
        List<String> children = zooKeeper.getChildren(TARGET_ZNODE, this);

        System.out.println("Data: " + new String(data) + " Children: " + children);

    }
}
