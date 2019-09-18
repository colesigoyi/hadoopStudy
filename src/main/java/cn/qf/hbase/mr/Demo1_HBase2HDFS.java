package cn.qf.hbase.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @ program: hadoopStudy
 * @ author:  TaoXueFeng
 * @ create: 2019-09-18 10:05
 * @ desc:
 **/

public class Demo1_HBase2HDFS extends ToolRunner implements Tool{

    private Configuration configuration;
    private static final String ZK_NAME="hbase.zookeeper.quorum";
    private static final String ZK_URL="10.211.55.28:2181,10.211.55.29:2181,10.211.55.30:2181";
    private static final String PLATFORM_NAME = "mapreduce.framework.name";
    private static final String PLATFORM_VALUE = "yarn";
    private static final String HDFS_CONNECT_KEY = "fs.defaultFS";
    private static final String HDFS_CONNECT_VALUE = "hdfs://linux9:9000";
    private static final String RM_HOSTNAME_KEY = "yarn.resourcemanager.hostname";
    private static final String RM_HOSTNAME_VALUE = "linux9";

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(configuration);

        TableMapReduceUtil.initTableMapperJob(
                "ns1:t_user", getScan(), HBaseMapper.class,Text.class,NullWritable.class,job);

        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        job.setJarByClass(Demo1_HBase2HDFS.class);
        job.waitForCompletion(true);
        return 0;
    }
    public Scan getScan() {
        return new Scan();
    }

    @Override
    public void setConf(Configuration configuration) {
        configuration.set(ZK_NAME, ZK_URL);
        configuration.set(PLATFORM_NAME, PLATFORM_VALUE);
        configuration.set(HDFS_CONNECT_KEY, HDFS_CONNECT_VALUE);
        configuration.set(RM_HOSTNAME_KEY, RM_HOSTNAME_VALUE);

        this.configuration = configuration;
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(),new Demo1_HBase2HDFS(), args);
    }
}
class HBaseMapper extends TableMapper<Text, NullWritable> {

    private Text k = new Text();
    //private Text v = new Text();

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        CellScanner cellScanner = value.cellScanner();
        StringBuffer stringBuffer = new StringBuffer();

        while (cellScanner.advance()) {
            Cell cell = cellScanner.current();
            String val = new String(CellUtil.cloneValue(cell));
            stringBuffer.append(val).append(",");
            k.set(stringBuffer.toString());
            //v.set(val);
            context.write(k, NullWritable.get());
        }
    }
}