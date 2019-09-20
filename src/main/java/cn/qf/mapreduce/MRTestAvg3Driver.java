package cn.qf.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MRTestAvg3Driver {

    public static void main(String[] args) throws Exception {

        Configuration conf1 = new Configuration();
        Configuration conf2 = new Configuration();

        Job job1 = Job.getInstance(conf1);
        Job job2 = Job.getInstance(conf2);

        job1.setJarByClass(MRTestAvg3Driver.class);
        job1.setMapperClass(MRMapper3_1.class);


        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(StudentBean.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(StudentBean.class);

        job1.setPartitionerClass(MyPartitioner.class);

        job1.setNumReduceTasks(4);

        Path inputPath = new Path("/Users/taoxuefeng/input");
        Path outputPath = new Path("/Users/taoxuefeng/output/output_hw3_1");

        FileInputFormat.setInputPaths(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, outputPath);

        job2.setMapperClass(MRMapper3_2.class);
        job2.setReducerClass(MRReducer3_2.class);

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(StudentBean.class);
        job2.setOutputKeyClass(StudentBean.class);
        job2.setOutputValueClass(NullWritable.class);

        Path inputPath2 = new Path("/Users/taoxuefeng/output/output_hw3_1");
        Path outputPath2 = new Path("/Users/taoxuefeng/output/output_hw3_end");

        FileInputFormat.setInputPaths(job2, inputPath2);
        FileOutputFormat.setOutputPath(job2, outputPath2);

        JobControl control = new JobControl("Score3");

        ControlledJob aJob = new ControlledJob(job1.getConfiguration());
        ControlledJob bJob = new ControlledJob(job2.getConfiguration());

        bJob.addDependingJob(aJob);

        control.addJob(aJob);
        control.addJob(bJob);

        Thread thread = new Thread(control);
        thread.start();

        while(!control.allFinished()) {
            thread.sleep(1000);
        }
        System.exit(0);


    }




    public static class MRMapper3_1 extends Mapper<LongWritable, Text, IntWritable, StudentBean>{

        StudentBean outKey = new StudentBean();
        IntWritable outValue = new IntWritable();
        List<String> scoreList = new ArrayList<String>();

        protected void map(LongWritable key, Text value, Context context) throws java.io.IOException ,InterruptedException {

            scoreList.clear();
            String[] splits = value.toString().split(",");
            long sum = 0;

            for(int i=2;i<splits.length;i++) {
                scoreList.add(splits[i]);
                sum += Long.parseLong(splits[i]);
            }

            Collections.sort(scoreList);
            outValue.set(Integer.parseInt(scoreList.get(scoreList.size()-1)));

            double avg = sum * 1.0/(splits.length-2);
            outKey.setCourse(splits[0]);
            outKey.setName(splits[1]);
            outKey.setavgScore(avg);

            context.write(outValue, outKey);

        };
    }



    public static class MRMapper3_2 extends Mapper<LongWritable, Text,IntWritable, StudentBean >{

        StudentBean outValue = new StudentBean();
        IntWritable outKey = new IntWritable();

        protected void map(LongWritable key, Text value, Context context) throws java.io.IOException ,InterruptedException {

            String[] splits = value.toString().split("\t");
            outKey.set(Integer.parseInt(splits[0]));

            outValue.setCourse(splits[1]);
            outValue.setName(splits[2]);
            outValue.setavgScore(Double.parseDouble(splits[3]));

            context.write(outKey, outValue);


        };
    }


    public static class MRReducer3_2 extends Reducer<IntWritable, StudentBean, StudentBean, NullWritable>{

        StudentBean outKey = new StudentBean();

        @Override
        protected void reduce(IntWritable key, Iterable<StudentBean> values,Context context)
                throws IOException, InterruptedException {

            int length = values.toString().length();

            for(StudentBean value : values) {
                outKey = value;
            }

            context.write(outKey, NullWritable.get());

        }
    }
    public static class MyPartitioner extends Partitioner<CourseScore3, NullWritable> {

        @Override
        public int getPartition(CourseScore3 courseScore, NullWritable nullWritable, int numPartitions) {

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
class CourseScore3 implements WritableComparable<CourseScore3> {

    private String course;
    private String name;
    private double score;

    public CourseScore3(String course, String name, double score) {
        super();
        this.course = course;
        this.name = name;
        this.score = score;
    }

    public CourseScore3() {
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
    public int compareTo(CourseScore3 cs) {

        int compareTo = this.course.compareTo(cs.getCourse());

        if (compareTo == 0) {
            double diff = cs.getScore() - this.score;
            if (diff >
                    0) {
                return 1;
            } else if (diff < 0) {
                return -1;
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

