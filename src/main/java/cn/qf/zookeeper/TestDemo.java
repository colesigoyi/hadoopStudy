package cn.qf.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @ program: mavenStudy
 * @ author:  TaoXueFeng
 * @ create: 2019-09-03 16:30
 * @ desc:
 **/

public class TestDemo {
    ZooKeeper zkClient = ZKUtils.getZkClient();
    @Test
    public void init() throws IOException, KeeperException, InterruptedException {

        List<String> childrens = zkClient.getChildren("/", true);
        for (String children : childrens) {
            System.out.println(children);
        }
    }

    /**
     * 创建节点
     */
    @Test
    public void create() {
        try {
            String s = zkClient.create("/app2", "helloworld".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println(s);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    /**
     * 删除节点
     */
    @Test
    public void delete() {

        try {
            //List<String> app1 = zkClient.getChildren("app1", true);
            zkClient.delete("/app2", 0);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
    /**
     * 查询节点的数据
     */
    @Test
    public void get() {
        Stat stat = new Stat();
        try {
            byte[] data = zkClient.getData("/app1", true, stat);
            System.out.println(new String(data));
            System.out.println(stat);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    /**
     *修改节点的数据
     */
    @Test
    public void set() {
        try {
            Stat stat = zkClient.setData("/app1", "哈哈哈".getBytes(), -1);
            System.out.println(stat);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
