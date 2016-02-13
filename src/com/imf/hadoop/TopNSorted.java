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

public class TopNSorted {
	public static class DataMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
		int[] topN;
		int len;

		@Override
		protected void setup(Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			len = context.getConfiguration().getInt("topn", 5);
			topN = new int[len + 1];
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			System.out.println("Mapper...");
			String[] data = value.toString().split(",");
			if (4 == data.length) {
				int cost = Integer.valueOf(data[2]);
				topN[0] = cost;
				Arrays.sort(topN);
			}
		}

		@Override
		protected void cleanup(Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			for (int i = 1; i < len; i++) {
				context.write(new IntWritable(topN[i]), new IntWritable(topN[i]));
			}
		}
	}

	public static class DataReducer extends Reducer<IntWritable, IntWritable, Text, IntWritable> {
		int[] topN;
		int len;
		@Override
		protected void setup(Reducer<IntWritable, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			len = context.getConfiguration().getInt("topn", 5);
			topN = new int[len + 1];
		}
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			System.out.println("Reducer...");
			topN[0] = key.get();
			Arrays.sort(topN);
		}
		@Override
		protected void cleanup(Reducer<IntWritable, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			for (int i = len; i >0; i--) {
				context.write(new Text(String.valueOf(len-i +1)), new IntWritable(topN[i]));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setInt("topn", 3);
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
		job.setOutputValueClass(Text.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
