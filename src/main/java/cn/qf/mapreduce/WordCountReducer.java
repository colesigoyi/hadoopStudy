package cn.qf.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @ program: mavenStudy
 * @ author:  TaoXueFeng
 * @ create: 2019-09-04 16:29
 * @ desc:
 **/

/**
 * KEYIN:输入的key的类型,对应mapper的keyout
 * VALUEIN:输入的value的类型,对应mapper的valueout
 * KEYOUT:输出的key的类型
 * VALUEOUT:输出的value的类型
 */
public class WordCountReducer extends Reducer< Text, IntWritable, Text, IntWritable> {
    /**
     * reduce方法每一组相同的mapper中输出的相同的key，调用一次reduce方法
     * @param key：输入的key
     * @param values：输出的value的值，对应mapper的输出
     * @param context：和mapper中一样
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //1、获取迭代器
        int count = 0;
        Iterator<IntWritable> iterator = values.iterator();
        //2、迭代
        while (iterator.hasNext()) {
            count += iterator.next().get();
        }
        //3、输出结果
        context.write(key, new IntWritable(count));
    }
}
