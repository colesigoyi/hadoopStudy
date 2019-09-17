package cn.qf.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @ program: hadoopStudy
 * @ author:  TaoXueFeng
 * @ create: 2019-09-16 16:59
 * @ desc:
 **/

public class HBaseUtils {
    private static final  String ZK_NAME="hbase.zookeeper.quorum";
    private static final  String ZK_URL="10.211.55.28:2181,10.211.55.29:2181,10.211.55.30:2181";

    public static Admin getAdmin() {
        //1. 获取到配置文件对象
        Configuration configuration = HBaseConfiguration.create();
        configuration.set(ZK_NAME, ZK_URL);
        //2. 获取到连接对象
        try {
            Connection connection = ConnectionFactory.createConnection(configuration);
            //3. 获取到admin对象
            Admin admin = connection.getAdmin();
            return admin;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
    public static void close(Admin admin) {
        if (admin != null) {
            Connection connection = admin.getConnection();
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
