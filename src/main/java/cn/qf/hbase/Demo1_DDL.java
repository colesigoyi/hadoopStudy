package cn.qf.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @ program: hadoopStudy
 * @ author:  TaoXueFeng
 * @ create: 2019-09-16 15:37
 * @ desc:
 **/
//10.211.55.28:2181,10.211.55.29:2181,10.211.55.30:2181
public class Demo1_DDL {
    private Admin admin;
    private Connection connection;
    @Before
    public void init() throws IOException {
        //1. 获取到配置文件对象
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum",
                "10.211.55.28:2181,10.211.55.29:2181,10.211.55.30:2181");
        //2. 获取到连接对象
        connection = ConnectionFactory.createConnection(configuration);
        //3. 获取到admin对象
        admin = connection.getAdmin();
    }
    @Test
    public void createNamespace() throws IOException {
        //创建namespace的描述器
        NamespaceDescriptor namespaceDescriptor =
                NamespaceDescriptor.create("bd1901").addConfiguration("SEX","MAN").build();
        //添加namespace
        admin.createNamespace(namespaceDescriptor);
    }
    @After
    public void close() throws IOException {
        if (connection != null) connection.close();
    }

    @Test
    public void listNamespace() throws IOException {
        //获取到hbase中的所有namespace的描述器
        NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
        //遍历
        for (NamespaceDescriptor descriptor : namespaceDescriptors) {
            System.out.println(descriptor.getName());
            System.out.println(descriptor.getConfigurationValue("SEX"));
        }
    }
    @Test
    public void listnamespaceTables() throws IOException {
        //获取到hbase中所有的namespace的描述器
        TableName[] tableNames = admin.listTableNamesByNamespace("ns1");
        //遍历
        for (TableName tableName : tableNames) {
            System.out.println(tableName.getName());
            System.out.println(tableName.getQualifier());
        }
    }
    @Test
    public void alter_namespace() throws IOException {
        //读取要求改的namespace的描述器
        NamespaceDescriptor namespaceDescriptor =
                admin.getNamespaceDescriptor("bd1901");
        //修改
        namespaceDescriptor.setConfiguration("xxx","ooo");
        //修改namespace的方法
        admin.modifyNamespace(namespaceDescriptor);
    }
    @Test
    public void drop_namespace() throws IOException {
        admin.deleteNamespace("bd1901");
    }
 }
