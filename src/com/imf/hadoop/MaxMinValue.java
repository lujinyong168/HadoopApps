package com.imf.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MaxMinValue {
	public static class DataMapper extends Mapper<Object, Text, Text, LongWritable> {
		private Text mykey = new Text("mykey");

		public void map( Object key, Text value, Context context) throws IOException, InterruptedException {
			System.out.println("Mapper...");
			context.write(mykey, new LongWritable(Long.parseLong(value.toString())));
		}
	}

	public static class DataReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		private long maxVal = Long.MIN_VALUE;
		private long minVal = Long.MAX_VALUE;

		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			System.out.println("Reducer...");
			for (LongWritable item : values) {
				if (item.get() > maxVal) {
					maxVal = item.get();
				}
				if (item.get() < minVal) {
					minVal = item.get();
				}
			}
				context.write(new Text("MaxVal:"), new LongWritable(maxVal));
				context.write(new Text("minVal:"), new LongWritable(minVal));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "MaxMinValue");
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
