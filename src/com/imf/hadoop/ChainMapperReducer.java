package com.imf.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
/**
 * 
 * @Description:链式MapReduce编程
 * @Author: lujinyong168
 * @Date: 2016年2月24日 上午6:28:34
 */
public class ChainMapperReducer {
	public static class DataMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			System.out.println("DataMapper1...");
			String line = value.toString().trim();
			if(line.length()>0){
				String[] splited = line.split("\t");
				int price = Integer.valueOf(splited[1]);
				if(price<10000){
					context.write(new Text(splited[0]), new IntWritable(price));
				}
			}
		}
	}
	public static class DataMapper2 extends Mapper<Text, IntWritable, Text, IntWritable> {
		public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
			System.out.println("DataMapper2...");
			if(value.get()>100){
				context.write(key,value);
			}
		}
	}
	public static class DataMapper3 extends Mapper<Text, IntWritable, Text, IntWritable> {
		public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
			System.out.println("DataMapper3...");
			if(value.get()>5000){
				context.write(key,value);
			}
		}
	}
	public static class DataReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			System.out.println("Reducer...");
			int sum = 0;
			for (IntWritable item : values) {
				sum += item.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: ChainMapperReducer <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "ChainMapperReducer");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(DataMapper1.class);
//		 job.setCombinerClass(DataCombiner.class);
		ChainMapper.addMapper(job, DataMapper1.class, LongWritable.class, Text.class, Text.class, IntWritable.class, new Configuration());
		ChainMapper.addMapper(job, DataMapper2.class, Text.class, IntWritable.class, Text.class, IntWritable.class, new Configuration());
		
		ChainReducer.setReducer(job, DataReducer.class, Text.class, IntWritable.class, Text.class, IntWritable.class,  new Configuration());
		ChainReducer.addMapper(job, DataMapper3.class, Text.class, IntWritable.class, Text.class, IntWritable.class, new Configuration());
		
		job.setReducerClass(DataReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
