package cn.qf.mapreduce;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class CourseGroupComparator  extends WritableComparator{

    public CourseGroupComparator() {
        super(CourseBean2.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CourseBean2 cb1 = (CourseBean2) a;
        CourseBean2 cb2 = (CourseBean2) b;

        return cb1.getCourse().compareTo(cb2.getCourse());
    }
}
