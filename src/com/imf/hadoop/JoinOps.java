package com.imf.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
/**
 * 
 * @Description:hadoop join操作
 * @Author: lujinyong168
 * @Date: 2016年2月16日 上午6:28:08
 */
public class JoinOps {
	public static class DataMapper extends Mapper<LongWritable, Text, LongWritable, WorkerInfo> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			System.out.println("Mapper map...");
			String inputData = value.toString();
			String[] splitedLine = inputData.split("\t");
			if(splitedLine.length<=3){//dept
				WorkerInfo dept = new WorkerInfo();
				dept.setDeptNo(splitedLine[0]);
				dept.setDeptName(splitedLine[1]);
				dept.setFlag(0);
				context.write(new LongWritable(Long.valueOf(dept.getDeptNo())),dept);
			}else{//worker
				WorkerInfo worker = new WorkerInfo();
				worker.setWorkerNo(splitedLine[0]);
				worker.setWorkerName(splitedLine[1]);
				worker.setDeptNo(splitedLine[7]);
				worker.setFlag(1);
				context.write(new LongWritable(Long.valueOf(worker.getDeptNo())),worker);
			}
		}
	}

	public static class DataReducer extends Reducer<LongWritable, WorkerInfo, LongWritable,Text> {
		public void reduce(Text key, Iterable<WorkerInfo> values, Context context)
				throws IOException, InterruptedException {
			LongWritable outKey = new LongWritable(0);
			Text outVal = new Text();
			System.out.println("Reducer reduce...");
			WorkerInfo dept = null;
			List<WorkerInfo> list = new ArrayList<WorkerInfo>();
			for (WorkerInfo item : values) {
				if(0==item.getFlag()){
					dept = new WorkerInfo(item);
				}else{
					list.add(new WorkerInfo(item));
				}
			}
			for (WorkerInfo worker : list) {
				worker.setDeptNo(dept.getDeptNo());
				worker.setDeptName(dept.getDeptName());
				outVal.set(worker.toString());
				context.write(outKey,outVal);
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
		Job job = Job.getInstance(conf, "JoinOps");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(DataMapper.class);
		// job.setCombinerClass(Averageducer.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(WorkerInfo.class);
		job.setReducerClass(DataReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
/**
 * 
 * @Description:自定义员工信息
 * @Author: lujinyong168
 * @Date: 2016年2月16日 上午6:37:27
 */
class WorkerInfo implements WritableComparable<Object>{
	private String workerNo;
	private String workerName;
	private String deptNo;
	private String deptName;
	private int flag = 0;//0 is department,1 is worker;default 0;

	public WorkerInfo(String workerNo, String workerName, String deptNo, String deptName, int flag) {
		super();
		this.workerNo = workerNo;
		this.workerName = workerName;
		this.deptNo = deptNo;
		this.deptName = deptName;
		this.flag = flag;
	}
	public WorkerInfo(WorkerInfo worker){
		this.workerNo = worker.getWorkerNo();
		this.workerName = worker.getWorkerName();
		this.deptNo = worker.getDeptNo();
		this.deptName = worker.getDeptName();
		this.flag = worker.getFlag();
	}
	public WorkerInfo() {	}

	@Override
	public void write(DataOutput out) throws IOException {
		System.out.println("WorkerInfo write...");
		out.writeUTF(this.workerNo);
		out.writeUTF(this.workerName);
		out.writeUTF(this.deptNo);
		out.writeUTF(this.deptName);
		out.writeInt(this.flag);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		System.out.println("WorkerInfo readFields...");
		this.setWorkerNo(in.readUTF());
		this.setWorkerName(in.readUTF());
		this.setDeptNo(in.readUTF());
		this.setDeptName(in.readUTF());
		this.setFlag(in.readInt());
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}
	public String toString(){
		return this.getWorkerNo()+"\t"+this.getWorkerName()+"\t"+this.getDeptNo()+"\t"+this.getDeptName();
		
	}
	public String getWorkerNo() {
		return workerNo;
	}

	public void setWorkerNo(String workerNo) {
		this.workerNo = workerNo;
	}

	public String getWorkerName() {
		return workerName;
	}

	public void setWorkerName(String workerName) {
		this.workerName = workerName;
	}

	public String getDeptNo() {
		return deptNo;
	}

	public void setDeptNo(String deptNo) {
		this.deptNo = deptNo;
	}

	public String getDeptName() {
		return deptName;
	}

	public void setDeptName(String deptName) {
		this.deptName = deptName;
	}

	public int getFlag() {
		return flag;
	}

	public void setFlag(int flag) {
		this.flag = flag;
	}
}
