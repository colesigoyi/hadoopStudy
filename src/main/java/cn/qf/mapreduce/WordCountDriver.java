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
 * @ program: mavenStudy
 * @ author:  TaoXueFeng
 * @ create: 2019-09-04 16:51
 * @ desc:  驱动程序
 **/

public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //
        Configuration configuration = new Configuration();
        //
        //configuration.set("mapreduce.framework.name", "yarn");
        //configuration.set("yarn.resourcemanager.hostname","linuxoffline");
        //

        Job job = Job.getInstance(configuration);
        //
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //
        //job.setJarByClass(WordCountDriver.class);
        job.setJar("/Users/taoxuefeng/Documents/02_StudyCoding/11_mavenStudy/hadoopStudy/target/hadoopStudy-1.0-SNAPSHOT.jar");
        //
        job.waitForCompletion(true);
    }
}
