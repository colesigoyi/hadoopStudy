package cn.qf.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;


/**
 * @ program: hadoopStudy
 * @author  TaoXueFeng
 * @ create: 2019-09-05 17:57
 * @ desc:
 **/

public class InfoJoinDriver extends ToolRunner implements Tool {
    private Configuration configuration = new Configuration();
    public static void main(String[] args) throws Exception {
        ToolRunner.run(null, new InfoJoinDriver(), args);
    }
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(configuration);
        job.setMapperClass(InfoJoinMapper.class);
        job.setReducerClass(InfoJoinReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(InfoJoinBean.class);
        job.setOutputKeyClass(InfoJoinBean.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setJar("/Users/taoxuefeng/Documents/02_StudyCoding/11_mavenStudy/" +
                "hadoopStudy/target/hadoopStudy-1.0-SNAPSHOT.jar");
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }
    public Configuration getConf() {
        return configuration;
    }
}
class InfoJoinMapper extends Mapper<LongWritable, Text, Text, InfoJoinBean> {
    private InfoJoinBean infoJoinBean = new InfoJoinBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取读取的文件的分片
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        String path = inputSplit.getPath().getName();
        String line = value.toString();
        String pids;
        if (path.startsWith("t_order")) {
            String[] fields = line.split(",");
            pids = fields[2];
            infoJoinBean.set(Integer.parseInt(fields[0]),fields[1],pids,Integer.parseInt(fields[3]),
                    "",0,0.0,"order");

        }else {
            String[] fields = line.split(",");
            pids = fields[0];
            infoJoinBean.set(0,"", pids,0,fields[1],Integer.parseInt(fields[2]),
                    Float.parseFloat(fields[3]),"product");
        }
        context.write(new Text(pids), infoJoinBean);
    }
}
class InfoJoinReducer extends Reducer< Text, InfoJoinBean, InfoJoinBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<InfoJoinBean> values, Context context) throws IOException, InterruptedException {
        InfoJoinBean pbean = new InfoJoinBean();
        Iterator<InfoJoinBean> iterator = values.iterator();
        ArrayList<InfoJoinBean> obeans = new ArrayList<InfoJoinBean>();
        while (iterator.hasNext()) {
            InfoJoinBean bean = iterator.next();
            if ("product".equals(bean.getFlag())) {
                pbean.setPid(bean.getPid());
                pbean.setPname(bean.getPname());
                pbean.setCategory_id(bean.getCategory_id());
                pbean.setPrice(bean.getPrice());
                pbean.setFlag(bean.getFlag());
                pbean.setOid(bean.getOid());
                pbean.setDate(bean.getDate());
                pbean.setAmount(bean.getAmount());

            }else {
                InfoJoinBean obean = new InfoJoinBean();
                obean.setPid(bean.getPid());
                obean.setPname(bean.getPname());
                obean.setCategory_id(bean.getCategory_id());
                obean.setPrice(bean.getPrice());
                obean.setFlag(bean.getFlag());
                obean.setOid(bean.getOid());
                obean.setDate(bean.getDate());
                obean.setAmount(bean.getAmount());
                obeans.add(obean);
            }
        }
        for (InfoJoinBean bean : obeans) {
            bean.setPname(pbean.getPname());
            bean.setCategory_id(pbean.getCategory_id());
            bean.setPrice(pbean.getPrice());
            context.write(bean, NullWritable.get());
        }
    }
}
