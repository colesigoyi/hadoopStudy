package cn.qf.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ program: mavenStudy
 * @author  TaoXueFeng
 * @ create: 2019-09-04 16:29
 * @ desc:
 **/

/**
 * KEYIN:输入的key的类型
 * VALUEIN:输入的value的类型
 * KEYOUT:输出的key的类型
 * VALUEOUT:输出的value的类型
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    /**
     * 每读取一行文本，这个map方法就被调用一次
     * @param key：对应的输入的key，在此处表示的就是行的编号
     * @param value：表示的输入的value。在此处表示行号对应的那一行的内容
     * @param context：上下文
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1、先将读取的这一行数据转换成String
        String line = value.toString();
        //2、切割单词
        String[] words = line.split(" ");
        //3、遍历
        for (String word : words) {
            //4、将单词作为key，出现就记录一次
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
