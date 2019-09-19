package cn.qf.hbase.coprocessor;

import cn.qf.hbase.HBaseUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import java.io.IOException;
import java.util.List;

/**
 * @ program: hadoopStudy
 * @ author:  TaoXueFeng
 * @ create: 2019-09-19 10:31
 * @ desc:
 **/

public class HBaseCoprocessor extends BaseRegionObserver {
    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability)
            throws IOException {
        //获取表对象
        Table guanzhuTable = HBaseUtils.getTable("guanzhu");
        //获取put对象(guanzhu表)的行键和列值
        byte[] rowkey = put.getRow();//获取列的cell
        List<Cell> cells = put.get("cf".getBytes(), "name".getBytes());
        Cell cell = cells.get(0);
        //获取列值
        byte[] value = CellUtil.cloneValue(cell);
        //获取到向fans表插入数据的put对象
        Put new_put = new Put(value);
        new_put.addColumn("cf".getBytes(),"star".getBytes(),rowkey);
        //提交表
        Table fansTable = HBaseUtils.getTable("fans");
        fansTable.put(new_put);
        //释放资源
        HBaseUtils.close(guanzhuTable);
        HBaseUtils.close(fansTable);
    }
}
