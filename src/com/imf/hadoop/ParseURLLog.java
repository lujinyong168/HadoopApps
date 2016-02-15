package com.imf.hadoop;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
/**
 * 
 * @Description:URL流量分析:统计不同访问方式（GET和POST），每种url访问次数
 * @Author: lujinyong168
 * @Date: 2016年2月15日 下午9:39:26
 */
public class ParseURLLog {
	public static class DataMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		Text outKey = new Text();
		LongWritable outVal = new LongWritable(1);
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			System.out.println("Mapper map...");
			String line = value.toString();
			String result = handleLine(line);
			if(result!=null && result.length()>0){
				outKey.set(result);
				context.write(outKey, outVal);
			}
		}
		/**
		 * 
		 * @Description:获取每行内容的output key
		 * @Auther: lujinyong168
		 * @Date: 2016年2月15日 下午9:45:49
		 */
		private String handleLine(String line) {
			StringBuffer buffer = new StringBuffer();
			if(line.length()>0){
				if(line.contains("GET")){
					buffer.append(line.substring(line.indexOf("GET"),line.indexOf("HTTP/1.0")).trim());
				}else if (line.contains("POST")){
					buffer.append(line.substring(line.indexOf("POST"),line.indexOf("HTTP/1.0")).trim());
				}
			}
			return buffer.toString();
		}
	}

	public static class DataReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable sum = new LongWritable(1);
		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			System.out.println("Reducer reduce...");
			int count = 0;
			for (LongWritable item : values) {
				count+= item.get();
			}
			sum.set(count);
			context.write(key, sum);
			
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "ParseURLLog");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(DataMapper.class);
		// job.setCombinerClass(Averageducer.class);
		job.setReducerClass(DataReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
