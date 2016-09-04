package com.imf.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
/**
 * 
 * @Description:多维排序
 * @Author: lujinyong168
 * @Date: 2016年2月23日 上午6:23:46
 */
public class MutipleSorting {
	public static class DataMapper extends Mapper<LongWritable, Text, IntMutiplePair, IntWritable> {
		private IntMutiplePair intMutiplePair = new IntMutiplePair();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			System.out.println("Mapper...");
//			String f = key.toString().trim();
			 String[] split = value.toString().trim().split("\t");
			 String f = split[0];
			int s = Integer.parseInt(split[1]);
			intMutiplePair.setFirst(f);
			intMutiplePair.setSecond(s);
			context.write(intMutiplePair, new IntWritable(s));
		}
	}
	public static class DataReducer extends Reducer<IntMutiplePair, IntWritable, Text, Text> {

		public void reduce(IntMutiplePair key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			System.out.println("Reducer...");
			StringBuffer buffer = new StringBuffer();
			for (IntWritable item : values) {
				buffer.append(item.get()+",");
			}
			String result = buffer.toString().substring(0,buffer.toString().length()-1);
			context.write(new Text(key.getFirst()), new Text(result));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "MutipleSorting");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(DataMapper.class);
//		 job.setCombinerClass(DataCombiner.class);
		job.setReducerClass(DataReducer.class);
		job.setMapOutputKeyClass(IntMutiplePair.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setPartitionerClass(MutipleSortingPartitioner.class);
		job.setSortComparatorClass(IntMultipleSortingComparator.class);
		job.setGroupingComparatorClass(GroupintMultipleSortingComparator.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
class IntMutiplePair implements WritableComparable<IntMutiplePair>{
	private String first;
	private int second;
	public IntMutiplePair(){}
	public IntMutiplePair(String first,int second){
		super();
		this.first = first;
		this.second = second;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.first);
		out.writeInt(this.second);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.first = in.readUTF();
		this.second = in.readInt();
		
	}

	@Override
	public int compareTo(IntMutiplePair o) {
		return 0;
	}
	public String getFirst() {
		return first;
	}
	public void setFirst(String first) {
		this.first = first;
	}
	public int getSecond() {
		return second;
	}
	public void setSecond(int second) {
		this.second = second;
	}
}
class IntMultipleSortingComparator extends WritableComparator{

	public IntMultipleSortingComparator() {
		super(IntMutiplePair.class,true);
		
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		IntMutiplePair x = (IntMutiplePair)a;
		IntMutiplePair y = (IntMutiplePair)b;
		System.out.println(x.getFirst()+":"+x.getSecond()+"       "+y.getFirst()+":"+y.getSecond());
		if(!x.getFirst().equals(y.getFirst())){
			return x.getFirst().compareTo(y.getFirst());
		}else{
			return x.getSecond()-y.getSecond();
		}
	}
	
}
class GroupintMultipleSortingComparator extends WritableComparator{

	public GroupintMultipleSortingComparator() {
		super(IntMutiplePair.class,true);
		
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		IntMutiplePair x = (IntMutiplePair)a;
		IntMutiplePair y = (IntMutiplePair)b;
		return x.getFirst().compareTo(y.getFirst());
	}
}
class MutipleSortingPartitioner extends Partitioner<IntMutiplePair, IntWritable>{

	@Override
	public int getPartition(IntMutiplePair key, IntWritable value, int numPartitions) {
		// TODO Auto-generated method stub
		return Math.abs((key.getFirst().hashCode()*127)%numPartitions);
	}
	
}
