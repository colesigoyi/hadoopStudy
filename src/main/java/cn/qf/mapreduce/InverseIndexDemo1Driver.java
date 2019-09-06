package cn.qf.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
import java.util.Iterator;

/**
 * @ program: hadoopStudy
 * @ author:  TaoXueFeng
 * @ create: 2019-09-06 19:45
 * @ desc:
 **/

public class InverseIndexDemo1Driver extends ToolRunner implements Tool{

        private Configuration configuration = new Configuration();
        public static void main(String[] args) throws Exception {
            ToolRunner.run(null, new InverseIndexDemo1Driver(), args);
        }
        public int run(String[] args) throws Exception {
            Job job = Job.getInstance(configuration);
            job.setMapperClass(InverseIndexDemo1Mapper.class);
            job.setReducerClass(InverseIndexDemo1Reducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
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
class InverseIndexDemo1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //获取读取的文件的分片
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        String fileName = inputSplit.getPath().getName();
        String line = value.toString();
        String[] words = line.split(" ");
        for (String word : words) {
            context.write(new Text(word +"-->"+ fileName + " "), new IntWritable(1));
        }
    }
}

class InverseIndexDemo1Reducer extends Reducer< Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int count = 0;
        Iterator<IntWritable> iterator = values.iterator();
        while (iterator.hasNext()) {
            count += iterator.next().get();
        }
        context.write(new Text(key), new IntWritable(count));
    }
}
