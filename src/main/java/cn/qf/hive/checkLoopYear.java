package cn.qf.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @ program: hadoopStudy
 * @ author:  TaoXueFeng
 * @ create: 2019-09-10 15:40
 * @ desc:
 **/

public class checkLoopYear extends UDF {
    public String evaluate(int year) {
        if(year % 3200 == 0) {
            return "不是闰年";
        }else if(year % 400 == 0) {
            return "是闰年";
        }else if(year % 100 == 0) {
            return "不是闰年";
        }else if(year % 4 == 0) {
            return "是闰年";
        }else {
            return "不是闰年";
        }
    }
}
