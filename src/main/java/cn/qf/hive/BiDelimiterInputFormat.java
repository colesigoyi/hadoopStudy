package cn.qf.hive;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class BiDelimiterInputFormat extends TextInputFormat {

	@Override
	public RecordReader<LongWritable, Text> getRecordReader(

	InputSplit genericSplit, JobConf job, Reporter reporter)

	throws IOException {

		reporter.setStatus(genericSplit.toString());
		// 用装饰模式来写一个自己的加强版recordreader
		// MyDemoRecordReader reader = new MyDemoRecordReader(new LineRecordReader(job, (FileSplit) genericSplit));
		// 改源码模式写的一个加强版recordreader
		BiRecordReader reader = new BiRecordReader(job, (FileSplit)genericSplit);
		return reader;

	}

	
	/**
	 * 装饰模式来写一个自己的加强版recordreader
	 * @author
	 *
	 */
	public static class MyDemoRecordReader implements

	RecordReader<LongWritable, Text> {

		LineRecordReader reader;
		Text text;
		public MyDemoRecordReader(LineRecordReader reader) {
			this.reader = reader;
			text = reader.createValue();
		}

		public void close() throws IOException {
			reader.close();
		}
		public LongWritable createKey() {

			return reader.createKey();
		}
		public Text createValue() {
			return new Text();
		}
		public long getPos() throws IOException {
			return reader.getPos();
		}
		public float getProgress() throws IOException {
			return reader.getProgress();

		}
		public boolean next(LongWritable key, Text value) throws IOException {
			
			boolean next = reader.next(key, text);
			if(next){
				text.toString().replaceAll("\\|\\|", "\\|");
				Text replaceText = new Text();
				value.set(replaceText);
			}
			return next;


		}
	}
}
