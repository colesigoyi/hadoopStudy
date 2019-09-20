package cn.qf.mapreduce;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ program: hadoopStudy
 * @ author:  TaoXueFeng
 * @ create: 2019-09-20 16:35
 * @ desc:
 **/

public class StudentBean implements WritableComparable<StudentBean> {
    private String course;
    private String name;
    private double avgScore;

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
    public double getavgScore() {
        return avgScore;
    }
    public void setavgScore(double avgScore) {
        this.avgScore = avgScore;
    }
    public StudentBean(String course, String name, double avgScore) {
        super();
        this.course = course;
        this.name = name;
        this.avgScore = avgScore;
    }
    public StudentBean() {
        super();
    }

    @Override
    public String toString() {
        return course + "\t" + name + "\t" + avgScore;
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        course = in.readUTF();
        name = in.readUTF();
        avgScore = in.readDouble();
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(course);
        out.writeUTF(name);
        out.writeDouble(avgScore);
    }
    @Override
    public int compareTo(StudentBean stu) {
        double diffent =  this.avgScore - stu.avgScore;
        if(diffent == 0) {
            return 0;
        }else {
            return diffent > 0 ? -1 : 1;
        }
    }


}
