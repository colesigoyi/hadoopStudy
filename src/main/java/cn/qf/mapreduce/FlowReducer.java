package cn.qf.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @ program: hadoopStudy
 * @ author:  TaoXueFeng
 * @ create: 2019-09-05 10:07
 * @ desc:
 **/

public class FlowReducer extends Reducer<Text, Text, Text, FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator = values.iterator();
        long upflow = 0l;
        long downflow = 0l;
        long total = 0l;
        while (iterator.hasNext()) {
            String updown = iterator.next().toString();
            String[] ud = updown.split(",");
            upflow += Long.parseLong(ud[0]);
            downflow += Long.parseLong(ud[1]);
        }
        context.write(key, new FlowBean(upflow, downflow));
    }
}
