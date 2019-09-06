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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @ program: hadoopStudy
 * @ author:  TaoXueFeng
 * @ create: 2019-09-06 20:35
 * @ desc:
 **/

public class InverseIndexDemo2Driver extends ToolRunner implements Tool {

    private Configuration configuration = new Configuration();
    public static void main(String[] args) throws Exception {
        ToolRunner.run(null, new InverseIndexDemo2Driver(), args);
    }
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(configuration);
        job.setMapperClass(InverseIndexDemo2Mapper.class);
        job.setReducerClass(InverseIndexDemo2Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

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
class InverseIndexDemo2Mapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split("-->");
        System.out.println("words[0]" + words[0]);
        System.out.println("words[1]" + words[1]);
        context.write(new Text(words[0]), new Text(words[1] + ";\t"));
    }
}

class InverseIndexDemo2Reducer extends Reducer< Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        StringBuilder stringBuilder = new StringBuilder();
        for (Text value : values) {
            stringBuilder.append(value.toString() + "\t");
            System.out.println(value);
        }
        context.write(new Text(key), new Text(stringBuilder.toString()));
    }
}
