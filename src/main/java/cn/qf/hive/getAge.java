package cn.qf.hive;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.junit.Test;

import java.util.Calendar;

/**
 * @ program: hadoopStudy
 * @author  TaoXueFeng
 * @ create: 2019-09-10 16:26
 * @ desc:
 **/

public class getAge extends UDF {
    public int evaluate(String birth) {
        if (StringUtils.isNotEmpty(birth)) {

            String[] split = birth.split("-");
            int year = Integer.parseInt(split[0]);
            int month = Integer.parseInt(split[1]);
            int day = Integer.parseInt(split[2]);

            Calendar instance = Calendar.getInstance();
            int now_year = instance.get(Calendar.YEAR);
            int now_month = instance.get(Calendar.MONTH);
            int now_day = instance.get(Calendar.DATE);

            if (now_month < month) {
                int age =  now_year - year - 1;
                return age;
            }else {
                int age = now_year - year;
                return age;
            }
        }
        return 0;
    }
    @Test
    public void testAge() {
        int age = evaluate("1992-08-07");
        System.out.println(age);
    }
}
