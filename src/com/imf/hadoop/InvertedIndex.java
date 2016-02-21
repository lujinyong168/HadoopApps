package com.imf.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
/**
 * 
 * @Description:倒排索引
 * @Author: lujinyong168
 * @Date: 2016年2月22日 上午6:27:08
 */
public class InvertedIndex {
	
	public static class DataMapper extends Mapper<LongWritable, Text, Text, Text> {
		private final Text number = new Text("1");
		private String fileName ;
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			FileSplit inputSplit = (FileSplit)context.getInputSplit();
			fileName = inputSplit.getPath().getName();
		}
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			System.out.println("Mapper...");
			String line = value.toString().trim();
			if(line.length()>0){
				StringTokenizer stringTokenizer = new StringTokenizer(line);
				while(stringTokenizer.hasMoreTokens()){
					String keyForCombiner = stringTokenizer.nextToken();
					context.write(new Text(keyForCombiner+":"+fileName), number);
				}
			}
		}
	}
	public static class DataCombiner extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			System.out.println("Combiner...");
			int sum = 0;
			for (Text item : values) {
				sum += Integer.valueOf(item.toString());
			}
			String[] splited = key.toString().split(":");
			context.write(new Text(splited[0]), new Text(splited[1]+":"+sum));
		}
	}
	public static class DataReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			System.out.println("Reducer...");
			StringBuffer result = new StringBuffer();
			for (Text item : values) {
				result.append(item+";");
			}
			context.write(key, new Text(result.toString().substring(0,result.length()-1)));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "InvertedIndex");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(DataMapper.class);
		 job.setCombinerClass(DataCombiner.class);
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
