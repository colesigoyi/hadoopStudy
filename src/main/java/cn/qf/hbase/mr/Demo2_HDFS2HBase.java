package cn.qf.hbase.mr;

import cn.qf.hbase.HBaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

/**
 * @ program: hadoopStudy
 * @ author:  TaoXueFeng
 * @ create: 2019-09-18 11:45
 * @ desc:
 **/

public class Demo2_HDFS2HBase extends ToolRunner implements Tool {
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
        job.setMapperClass(HDFSMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        TableMapReduceUtil.initTableReducerJob("ns1:t2",HDFSReducer.class,job);

        job.setJarByClass(Demo2_HDFS2HBase.class);
        job.waitForCompletion(true);
        return 0;
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
    public Scan getScan() {
        return new Scan();
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(),new Demo2_HDFS2HBase(), args);
    }

}
class HDFSMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text k = new Text();
    private Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");
        String rowKey = UUID.randomUUID().toString().replace("-","").toUpperCase();
        k.set(rowKey);
        String age = "age:" + fields[0];
        String name = "name:" + fields[1];
        String sex = "sex:" + fields[2];
        String height = "height:" + fields[3];
        v.set(age);
        context.write(k, v);
        v.set(name);
        context.write(k, v);
        v.set(sex);
        context.write(k, v);
        v.set(height);
        context.write(k, v);
    }
}
class HDFSReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

    Admin admin = HBaseUtils.getAdmin();
    TableName tableName = TableName.valueOf("ns1:t2");
    String family = "base_info";

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        if (!admin.tableExists(tableName)) {
            //创建表的描述器
            HTableDescriptor tableDescriptor =
                    new HTableDescriptor(tableName);
            HColumnDescriptor columnDescriptor = new HColumnDescriptor("base_info");
            columnDescriptor.setBloomFilterType(BloomType.ROW);
            columnDescriptor.setVersions(1,3);
            tableDescriptor.addFamily(columnDescriptor);
            admin.createTable(tableDescriptor);
        }
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Table table = HBaseUtils.getTable("ns1:t2");
        List<Put> plist = new ArrayList<Put>();

        Iterator<Text> iterator = values.iterator();
        while (iterator.hasNext()) {
            Put put = new Put(key.toString().getBytes());
            String kv = iterator.next().toString();
            String column = kv.split(":")[0];
            String value = kv.split(":")[1];
            put.addColumn(
                    family.getBytes(),column.getBytes(),value.getBytes());
            plist.add(put);
        }
        if (table != null) {
            table.put(plist);
        }
    }
}

