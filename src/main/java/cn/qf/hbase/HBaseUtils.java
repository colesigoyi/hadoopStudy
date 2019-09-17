package cn.qf.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import sun.tools.jconsole.Tab;

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

    private static Configuration configuration;
    private static Connection connection;

    private static final String DEFAULT_TABLE_NAME = "ns1:t1";

    static {
        //1. 获取到配置文件对象
        configuration = HBaseConfiguration.create();
        configuration.set(ZK_NAME, ZK_URL);
        //2. 获取到连接对象
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static Admin getAdmin() {

        try {
            //3. 获取到admin对象
            Admin admin = connection.getAdmin();
            return admin;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static Table getTable(String tableName) {
        try {
            TableName table_Name = TableName.valueOf(tableName);
            if (getAdmin().tableExists(table_Name)) {
                Table table = connection.getTable(TableName.valueOf(tableName));
                return table;
            }else {
                return null;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
    public static Table getTable() {
        Table table = getTable(DEFAULT_TABLE_NAME);
        return table;
    }

    public static void checkConnection() throws IOException {
        if (connection.isClosed()) {
            Connection connection = ConnectionFactory.createConnection(configuration);
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
    public static void close(Table table) {
        try {
            if (table != null) {
                table.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
