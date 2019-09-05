package cn.qf.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ program: hadoopStudy
 * @ author:  TaoXueFeng
 * @ create: 2019-09-05 10:07
 * @ desc:
 **/

public class FlowMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");
        String phone = fields[1];
        String upFlow = fields[fields.length - 3];
        String downFlow = fields[fields.length - 2];
        context.write(new Text(phone), new Text(upFlow + "," + downFlow));
    }
}
