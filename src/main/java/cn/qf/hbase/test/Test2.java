package cn.qf.hbase.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
/**
 * @ program: hadoopStudy
 * @ author:  TaoXueFeng
 * @ create: 2019-09-27 14:56
 * @ desc:
 **/

public class Test2 {
    private Admin admin;
    private Connection connection;
    public static TableName tName = TableName.valueOf("bd1901:t_course");
    public static Random ra = new Random();
    @Before
    public void init() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "10.211.55.28:2181,10.211.55.29:2181,10.211.55.30:2181");
        connection = ConnectionFactory.createConnection(configuration);
        admin = connection.getAdmin();
    }
    @Test
    public void create() throws IOException{
        if(admin.tableExists(tName)){
            admin.disableTable(tName);
            admin.deleteTable(tName);
        }
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tName);
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("sc".getBytes());

        hTableDescriptor.addFamily(hColumnDescriptor);
        admin.createTable(hTableDescriptor);
    }

    @Test
    public void insert() throws IOException{
        Table table = connection.getTable(tName);
        List<Put> putList = new ArrayList<Put>();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for(int i=1;i<10;i++){
            String id=format.format(new Date());
            id=id.replace("-", "").replace(" ", "").replace(":", "");
            id = "course"+getRowKey(id);
            Put put = new Put(id.getBytes());
            String name="Course"+i;
            put.addColumn("sc".getBytes(), "name".getBytes(), name.getBytes());
            putList.add(put);
        }
        table.put(putList);
    }

    @After
    public void after() throws IOException {
        if(connection!=null){
            connection.close();
        }
    }
    private String getRowKey(String id){
        return id+""+ra.nextInt(99999);
    }
}