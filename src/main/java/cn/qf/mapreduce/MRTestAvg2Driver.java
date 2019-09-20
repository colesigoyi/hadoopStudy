package cn.qf.mapreduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ program: hadoopStudy
 * @ author:  TaoXueFeng
 * @ create: 2019-09-20 16:33
 * @ desc:
 **/

public class MRTestAvg2Driver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        Job job= Job.getInstance(conf);
        job.setMapperClass(Mapper_CS.class);
        job.setReducerClass(Reducer.class);
        job.setMapOutputKeyClass(CourseScore.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(CourseScore.class);
        job.setOutputValueClass(NullWritable.class);

        /**
         * 设置reduceTask数量和分区器
         */
        job.setNumReduceTasks(4);
        job.setPartitionerClass(MyPartitioner.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJar("/Users/taoxuefeng/Documents/02_StudyCoding/11_mavenStudy/" +
                "hadoopStudy/target/hadoopStudy-1.0-SNAPSHOT.jar");

        job.waitForCompletion(true);
    }

    private static class Mapper_CS extends Mapper<LongWritable, Text, CourseScore, NullWritable> {

        CourseScore keyOut = new CourseScore();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] splits = value.toString().split(",");
            String course = splits[0];
            String name = splits[1];

            int sum = 0;
            int num = 0;
            for(int i=2; i<splits.length; i++){
                sum += Integer.valueOf(splits[i]);
                num ++;
            }
            double avgScore = Math.round(sum * 1D / num * 10) / 10D;

            keyOut.setCourse(course);
            keyOut.setName(name);
            keyOut.setScore(avgScore);

            context.write(keyOut, NullWritable.get());
        }
    }

    public static class MyPartitioner extends Partitioner<CourseScore, NullWritable> {

        @Override
        public int getPartition(CourseScore courseScore, NullWritable nullWritable, int numPartitions) {

            String course = courseScore.getCourse();
            if(course.equals("computer")){
                return 0;
            }else if(course.equals("english")){
                return 1;
            }else if(course.equals("algorithm")){
                return 2;
            }else{
                return 3;
            }
        }
    }

}



class CourseScore implements WritableComparable<CourseScore> {

    private String course;
    private String name;
    private double score;

    public CourseScore(String course, String name, double score) {
        super();
        this.course = course;
        this.name = name;
        this.score = score;
    }

    public CourseScore() {
    }

    public String getCourse() {
        return course;
    }

    public void setCourse(String course) {
        this.course = course;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // TODO Auto-generated method stub
        out.writeUTF(course);
        out.writeUTF(name);
        out.writeDouble(score);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO Auto-generated method stub
        this.course = in.readUTF();
        this.name = in.readUTF();
        this.score = in.readDouble();
    }

    /**
     * 排序规则
     * compareTo方法既充当排序用，用充当分组规则
     */
    @Override
    public int compareTo(CourseScore cs) {

        int compareTo = this.course.compareTo(cs.getCourse());

        if (compareTo == 0) {
            double diff = cs.getScore() - this.score;
            if (diff >
                    0) {
                return 1;
            } else if (diff < 0) {return -1;
            } else {
                return 0;
            }
        } else {
            return compareTo;
        }
    }


    @Override
    public String toString() {
        return course + "\t" + name + "\t" + score;
    }

}