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
import java.util.Iterator;

/**
 * @ program: mavenStudy
 * @ author:  TaoXueFeng
 * @ create: 2019-09-04 17:52
 * @ desc:
 **/

public class TempDriver {
    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {
        //
        Configuration configuration = new Configuration();
        //
        configuration.set("mapreduce.framework.name", "yarn");
        configuration.set("yarn.resourcemanager.hostname","linuxoffline");
        //

        Job job = Job.getInstance(configuration);
        //
        job.setMapperClass(TemMapper.class);
        job.setReducerClass(TemReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //
        job.setJarByClass(WordCountDriver.class);
        //
        job.waitForCompletion(true);
    }
}

class TemMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String data = line.substring(0, 8);
        String tem = line.substring(8);
        context.write(new Text(data), new Text(tem));
    }
}

class TemReducer extends Reducer< Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        double max = Double.MIN_VALUE;
        double avg = 0.0;
        int count = 0;
        double sum = 0;
        Iterator<Text> iterator = values.iterator();
        while (iterator.hasNext()) {
            String tem = iterator.next().toString();
            double tem_double = Double.parseDouble(tem);
            sum += tem_double;
            if (tem_double > max) {
                max = tem_double;
            }
            count++;
        }
        avg = sum / count;
        context.write(new Text(key.toString() + ":max"), new Text(max + ""));
        context.write(new Text(key.toString() + ":avg"), new Text(avg + ""));
    }
}
