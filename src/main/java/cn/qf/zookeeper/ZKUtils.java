package cn.qf.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * @ program: mavenStudy
 * @author  TaoXueFeng
 * @ create: 2019-09-03 17:04
 * @ desc: Zookeeper工具类
 **/

public class ZKUtils {
    private static ZooKeeper zkClient;
    static {
        //1、获取zookeeper的客户端对象
        String connection =
                "10.211.55.28:2181," +
                        "10.211.55.29:2181," +
                        "10.211.55.30:2181";
        int sessionTimeOut = 5000;

        try {
            zkClient = new ZooKeeper(
                    connection, sessionTimeOut, new Watcher() {
                public void process(WatchedEvent event) {
                    if (event.getType() == Event.EventType.NodeCreated) {
                        System.out.println("-------------");
                    }
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    static ZooKeeper getZkClient() {
        return zkClient;
    }
}
