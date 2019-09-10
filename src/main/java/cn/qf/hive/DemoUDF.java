package cn.qf.hive;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @ program: hadoopStudy
 * @ author:  TaoXueFeng
 * @ create: 2019-09-10 15:01
 * @ desc:
 **/

public class DemoUDF extends UDF {
    public String evaluate(String str) {
        if (StringUtils.isNotEmpty(str)) {
            return str.toUpperCase();
        }
        return null;
    }
}
