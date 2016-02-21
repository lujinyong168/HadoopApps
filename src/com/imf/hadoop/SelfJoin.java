package com.imf.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
/**
 * 
 * @Description:hadoop的自连接
 * @Author: lujinyong168
 * @Date: 2016年2月21日 上午6:34:57
 */
public class SelfJoin {
	public static class DataMapper extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			System.out.println("Mapper...");
			String[] splited = value.toString().split("\t");
			context.write(new Text(splited[1]), new Text("1_"+splited[0]));//left table
			context.write(new Text(splited[0]), new Text("0_"+splited[1]));//right table
		}
	}

	public static class DataReducer extends Reducer<Text, Text, NullWritable, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			System.out.println("Reducer...");
			Iterator<Text> iterator = values.iterator();
			List<String> grandChildList = new ArrayList<String>();
			List<String> grandParentList = new ArrayList<String>();
			while(iterator.hasNext()){
				String item = iterator.next().toString();
				String[] splited = item.split("_");
				if(splited[0].equals("1")){
					grandChildList.add(splited[1]);
				}else{
					grandParentList.add(splited[1]);
				}
			}
			if(grandChildList.size()>0&&grandParentList.size()>0){
				for (String grandChild : grandChildList) {
					for (String grandParent : grandParentList) {
						context.write(NullWritable.get(),new Text(grandChild+"\t"+grandParent));
					}
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "SelfJoin");
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
