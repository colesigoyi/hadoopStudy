package cn.qf.hbase;

import org.apache.hadoop.hbase.client.*;
import org.junit.Test;

import java.io.IOException;


/**
 * @ program: hadoopStudy
 * @ author:  TaoXueFeng
 * @ create: 2019-09-17 09:41
 * @ desc:
 **/

public class Demo3_DML {
    /**
     * 插入数据
     */
    @Test
    public void put() {
        //获取table对象
        Table table = HBaseUtils.getTable();
        //创建put对象
        Put put = new Put("001".getBytes());
        put.addColumn("base_info".getBytes(),
                "name".getBytes(),"txf2".getBytes());
        //插入一条数据
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * 获取数据
     */
    @Test
    public void get() {
        //获取table对象
        Table table = HBaseUtils.getTable();
        //创建get对象
        Get get = new Get("001".getBytes());

        try {
            //通过get获取结果
            Result result = table.get(get);
            HBaseUtils.showResult(result);
            HBaseUtils.close(table);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * 扫描
     */
    @Test
    public void scan() {
        //获取table对象
        Table table = HBaseUtils.getTable("ns1:t_user");
        //获取scan对象
        Scan scan = new Scan();
        try {
            //获取结果扫描器
            ResultScanner scanner = table.getScanner(scan);
            HBaseUtils.show(scanner);
            HBaseUtils.close(table);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * 删除
     */
    @Test
    public void delete() {
        //获取table对象
        Table table = HBaseUtils.getTable();
        //
        Delete delete = new Delete("002".getBytes());

        try {
            table.delete(delete);
            HBaseUtils.close(table);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * 添加(追加)
     */
    @Test
    public void append() {
        //获取table对象
        Table table = HBaseUtils.getTable();
        Append append = new Append("001".getBytes());
        try {
            table.append(append.add("base_info".getBytes(),
                    "age".getBytes(),"99".getBytes()));
            HBaseUtils.close(table);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * 自增长
     */
    @Test
    public void incr() {
        //获取table对象
        Table table = HBaseUtils.getTable();
        Increment increment = new Increment("001".getBytes());

        try {
            table.increment(increment.addColumn("base_info".getBytes(),"id".getBytes(), 1L));
            HBaseUtils.close(table);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
