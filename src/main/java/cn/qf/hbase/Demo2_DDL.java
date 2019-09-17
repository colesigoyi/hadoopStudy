package cn.qf.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.junit.Test;

import java.io.IOException;

/**
 * @ program: hadoopStudy
 * @ author:  TaoXueFeng
 * @ create: 2019-09-16 17:04
 * @ desc:
 **/

public class Demo2_DDL {
    @Test
    public void create() throws IOException {
        //获取admin对象
        Admin admin = HBaseUtils.getAdmin();
        //创建表描述器
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("ns1:t2"));
        //为这个表描述器添加列簇
        HColumnDescriptor columnDescriptor = new HColumnDescriptor("base_info");
        tableDescriptor.addFamily(columnDescriptor);
        //建表
        assert admin != null;
        admin.createTable(tableDescriptor);
        //释放资源
        HBaseUtils.close(admin);
    }
    @Test
    public void listByTableNames() throws IOException {
        //获取admin对象
        Admin admin = HBaseUtils.getAdmin();
        //获取到所有的表名称对象
        TableName[] tableNames = admin.listTableNames();
        //遍历
        for (TableName tableName : tableNames) {
            System.out.println(tableName.getNamespaceAsString());
            System.out.println(tableName.getNameAsString());
            System.out.println(tableName.getQualifierAsString());
        }
    }
    @Test
    public void listByTableDescriptor() throws IOException {
        //获取admin
        Admin admin = HBaseUtils.getAdmin();
        //获取到表的描述器对象
        HTableDescriptor[] tableDescriptors = admin.listTables();
        //遍历
        for (HTableDescriptor tableDescriptor : tableDescriptors) {
            System.out.println(tableDescriptor.getNameAsString());
            HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
            for (HColumnDescriptor columnDescriptor : columnFamilies) {
                System.out.println(columnDescriptor.getNameAsString());
                System.out.println(columnDescriptor.getBlocksize());
                System.out.println(columnDescriptor.getMaxVersions());
            }
        }
        HBaseUtils.close(admin);
    }
    @Test
    public void alter1() throws IOException {
        //获取admin对象
        Admin admin = HBaseUtils.getAdmin();
        //获取表的描述器对象
        HTableDescriptor tableDescriptor =
                admin.getTableDescriptor(TableName.valueOf("ns1:t1"));
        //添加一个列簇
        HColumnDescriptor columnDescriptor = new HColumnDescriptor("extra_info");
        tableDescriptor.addFamily(columnDescriptor);
        //提交修改
        admin.modifyTable(TableName.valueOf("ns1:t1"),tableDescriptor);
        //释放资源
        HBaseUtils.close(admin);
    }
    @Test
    public void alter2() throws IOException {
        //获取admin对象
        Admin admin = HBaseUtils.getAdmin();
        //获取到表的描述器对象
        HTableDescriptor tableDescriptor =
                admin.getTableDescriptor(TableName.valueOf("ns1:t1"));
        //删除一个列簇
        tableDescriptor.removeFamily("extra_info".getBytes());
        //提交修改
        admin.modifyTable(TableName.valueOf("ns1:t1"),tableDescriptor);
        //释放资源
        HBaseUtils.close(admin);
    }
    @Test
    public void drop() throws IOException {
        //获得admin对象
        Admin admin = HBaseUtils.getAdmin();
        //判断表是否存在
        TableName tableName = TableName.valueOf("ns1:t2");
        if (admin.tableExists(tableName)) {
            //如果表存在就将表设置为失效
            admin.disableTable(tableName);
            //判断一下表是否失效
            if (admin.isTableAvailable(tableName)) {
                //删除表
                admin.deleteTable(tableName);
            }

        }

    }
}


