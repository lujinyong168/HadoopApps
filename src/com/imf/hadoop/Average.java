package com.imf.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Average {
	public static class DataMapper extends Mapper<Object, Text, Text, FloatWritable> {
		private Text result = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			System.out.println("Mapper...");
			String data = value.toString();
			StringTokenizer itr = new StringTokenizer(data,"\n");
			while(itr.hasMoreElements()){
				StringTokenizer recored = new StringTokenizer(itr.nextToken());
				String name = recored.nextToken();
				String score = recored.nextToken();
				result.set(name);
				context.write(result, new FloatWritable(Float.valueOf(score)));
			}
		}
	}

	public static class DataReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
		private FloatWritable result = new FloatWritable();

		public void reduce(Text key, Iterable<FloatWritable> values, Context context)
				throws IOException, InterruptedException {
			System.out.println("Reducer...");
			float sum = 0;
			int count = 0;
			float avg = 0;
			for (FloatWritable val : values) {
				sum += val.get();
				count ++;
			}
			if(count>0){
				avg = sum/count;
			}
			result.set(avg);
			context.write(key, result);
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Average");
		job.setJarByClass(Average.class);
		job.setMapperClass(DataMapper.class);
		//job.setCombinerClass(Averageducer.class);
		job.setReducerClass(DataReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
