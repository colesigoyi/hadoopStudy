package cn.qf.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ program: hadoopStudy
 * @ author:  TaoXueFeng
 * @ create: 2019-09-05 10:07
 * @ desc:
 **/

public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //
        Configuration configuration = new Configuration();
        //
        configuration.set("mapreduce.framework.name", "yarn");
        configuration.set("yarn.resourcemanager.hostname","linux9");
        //mr的应用程序

        Job job = Job.getInstance(configuration);
        //
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        //job.setMapOutputKeyClass(Text.class);
        //job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //
        job.setJarByClass(FlowDriver.class);
        //
        job.waitForCompletion(true);
    }
}
