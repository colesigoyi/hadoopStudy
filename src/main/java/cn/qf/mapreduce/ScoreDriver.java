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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

/**
 * @ program: hadoopStudy
 * @ author:  TaoXueFeng
 * @ create: 2019-09-05 12:56
 * @ desc:
 **/

public class ScoreDriver extends ToolRunner implements Tool {
    private Configuration configuration = new Configuration();
    public static void main(String[] args)
            throws Exception {
        ToolRunner.run(null, new ScoreDriver(), args);
    }

    public int run(String[] args) throws Exception {
        //mr的应用程序
        Job job = Job.getInstance(configuration);
        job.setMapperClass(ScoreMapper.class);
        job.setReducerClass(ScoreReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setJar("/Users/taoxuefeng/Documents/02_StudyCoding/11_mavenStudy/hadoopStudy/target/hadoopStudy-1.0-SNAPSHOT.jar");
        return job.waitForCompletion(true) == true ? 0: 1;
    }

    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }

    public Configuration getConf() {
        return configuration;
    }
}
class ScoreMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(" ");
        double score = Double.parseDouble(fields[1]);
        String id = fields[0];
        String count = null;
        if (score < 60) {
            count = "<60";
        }else if (score < 70) {
            count = "60-69";
        }else if (score < 80) {
            count = "70-79";
        }else if (score < 90) {
            count = "80-89";
        }else if (score <= 100) {
            count = "90-100";
        }
        context.write(new Text(count), new IntWritable(1));

        //for (String word : fields) {
        //    //4、将单词作为key，出现就记录一次
        //    context.write(new Text(word), new IntWritable(1));
        //}
    }
}
class ScoreReducer extends Reducer<Text, IntWritable, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<IntWritable> iterator = values.iterator();
        int count = 0;
        int num = 0;
        double avg = 0.0;
        while (iterator.hasNext()) {
            count += iterator.next().get();
        }
        avg = (count % 10) * 10;
        context.write(key, new Text(count + " " + avg + "%"));
    }
}