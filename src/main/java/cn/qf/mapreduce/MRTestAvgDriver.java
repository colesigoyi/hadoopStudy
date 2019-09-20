package cn.qf.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;


public class MRTestAvgDriver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        Job job = Job.getInstance(conf);
        job.setJarByClass(MRTestAvgDriver.class);
        job.setMapperClass(mymapper.class);
        job.setReducerClass(myreducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJar("/Users/taoxuefeng/Documents/02_StudyCoding/11_mavenStudy/" +
                "hadoopStudy/target/hadoopStudy-1.0-SNAPSHOT.jar");

        job.waitForCompletion(true);
    }

    static class mymapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            String object = fields[0];
            StringBuffer score = new StringBuffer();
            for (int i = 2; i < fields.length; i++) {
                score.append(fields[i]);
                if (i != fields.length - 1) {
                    score.append("\t");
                }
            }
            context.write(new Text(object), new Text(score.toString()));
        }
    }

    static class myreducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            double avgofone;
            double avg;
            int sumall = 0;
            for (Text text : values) {
                String[] str = text.toString().split("\t");
                int sum = 0;
                int num = 0;
                for (String string : str) {
                    sum += Integer.parseInt(string);
                    num++;
                }
                avgofone=(sum/num);
                count++;
                sumall+=avgofone;
            }
            avg=sumall/count;
            String outString=count+"\t"+avg;
            Text t=new Text(outString);
            context.write(key, t);
        }
    }
}