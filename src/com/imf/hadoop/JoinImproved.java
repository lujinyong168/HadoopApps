package com.imf.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
/**
 * 
 * @Description:join操作晋级练习
 * @Author: lujinyong168
 * @Date: 2016年2月18日 上午6:24:31
 */
public class JoinImproved {
	public static class DataMapper extends Mapper<LongWritable, Text, MemberKey, MemberInfo> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			System.out.println("Mapper map...");
			String[] splitedLine = value.toString().split("\t");
			if(splitedLine.length==2){//address
				MemberInfo member = new MemberInfo();
				member.setAddressNo(splitedLine[0]);
				member.setAddressName(splitedLine[1]);
				
				MemberKey mk = new MemberKey();
				mk.setKeyID(Integer.valueOf(splitedLine[0]));
				mk.setFlag(false);
				context.write(mk, member);
			}else{//member
				MemberInfo member = new MemberInfo();
				member.setMemberNo(splitedLine[0]);
				member.setMemberName(splitedLine[1]);
				member.setAddressNo(splitedLine[2]);
				
				MemberKey mk = new MemberKey();
				mk.setKeyID(Integer.valueOf(splitedLine[2]));
				mk.setFlag(true);
				context.write(mk, member);
			}
		}
	}

	public static class DataReducer extends Reducer<MemberKey, MemberInfo, NullWritable,Text> {
		public void reduce(Text key, Iterable<MemberInfo> values, Context context)
				throws IOException, InterruptedException {
			System.out.println("Reducer reduce...");
			boolean flag =true;
			MemberInfo member = new MemberInfo();
			for (MemberInfo item : values) {
				if(flag){
					member = new MemberInfo(item);
					flag = false;
				}else{
					MemberInfo mem = new MemberInfo(item);
					mem.setAddressName(member.getAddressName());
					context.write(NullWritable.get(),new Text(mem.toString()));
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
		Job job = Job.getInstance(conf, "JoinImproved");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(DataMapper.class);
//		 job.setCombinerClass(DataReducer.class);
		job.setMapOutputKeyClass(MemberKey.class);
		job.setMapOutputValueClass(MemberInfo.class);
		job.setReducerClass(DataReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

class GroupComparator extends WritableComparator{
	public GroupComparator() {
		super(MemberKey.class,true);
		System.out.println("GroupComparator ... ");
	}
//	public GroupComparator(MemberKey memberKey){
//		super(memberKey.getClass());
//		System.out.println("GroupComparator 222... ");
//	}
	@Override
	public int compare(WritableComparable a,WritableComparable b){
		System.out.println("GroupComparator compare  ... ");
		MemberKey x = (MemberKey)a;
		MemberKey y = (MemberKey)b;
		if(x.getKeyID()==y.getKeyID()){
			return 0;
		}else{
			return x.getKeyID()>y.getKeyID()?1:-1;
		}
	}
}
/**
 * 
 * @Description:自定义Key
 * @Author: lujinyong168
 * @Date: 2016年2月18日 上午6:27:44
 */
class MemberKey implements WritableComparable<MemberKey>{
	private int keyID;
	private boolean  flag;//标记是否是member，false不是，true是；
	public MemberKey(){}
	public MemberKey(int keyID, boolean flag) {
		super();
		this.keyID = keyID;
		this.flag = flag;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.keyID);
		out.writeBoolean(this.flag);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.keyID = in.readInt();
		this.flag = in.readBoolean();
		
	}

	@Override
	public int compareTo(MemberKey o) {
		if(this.keyID == o.keyID){
			if(this.flag==o.flag){
				return 0;
			}else{
				return this.flag?-1:1;
			}
		}else{
			return this.keyID>o.keyID?1:-1;
		}
	}
	@Override
	public int hashCode() {
		return keyID;
	}
	public int getKeyID() {
		return keyID;
	}

	public void setKeyID(int keyID) {
		this.keyID = keyID;
	}

	public boolean isFlag() {
		return flag;
	}

	public void setFlag(boolean flag) {
		this.flag = flag;
	}
}
/**
 * 
 * @Description:自定义member
 * @Author: lujinyong168
 * @Date: 2016年2月18日 上午6:27:02
 */
class MemberInfo implements WritableComparable<Object>{
	private String memberNo="";
	private String memberName="";
	private String addressNo="";
	private String addressName="";
//	private int flag = 0;//0 is address;1 is member
	public MemberInfo(String memberNo, String memberName, String addressNo,String addressName) {
		super();
		this.memberNo = memberNo;
		this.memberName = memberName;
		this.addressNo = addressNo;
		this.addressName = addressName;
//		this.flag = flag;
	}
	public MemberInfo() {	}

	public MemberInfo(MemberInfo m){
		this.memberNo = m.getMemberNo();
		this.memberName = m.getMemberName();
		this.addressNo = m.getAddressNo();
		this.addressName = m.getAddressName();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		System.out.println("MemberInfo write...");
		out.writeUTF(this.memberNo);
		out.writeUTF(this.memberName);
		out.writeUTF(this.addressNo);
		out.writeUTF(this.addressName);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		System.out.println("MemberInfo readFields...");
		this.setMemberNo(in.readUTF());
		this.setMemberName(in.readUTF());
		this.setAddressNo(in.readUTF());
		this.setAddressName(in.readUTF());
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}
	public String toString(){
		return this.getMemberNo()+"\t"+this.getMemberName()+"\t"+this.addressName;
		
	}
	public String getMemberNo() {
		return memberNo;
	}
	public void setMemberNo(String memberNo) {
		this.memberNo = memberNo;
	}
	public String getMemberName() {
		return memberName;
	}
	public void setMemberName(String memberName) {
		this.memberName = memberName;
	}
	public String getAddressNo() {
		return addressNo;
	}
	public void setAddressNo(String addressNo) {
		this.addressNo = addressNo;
	}
	public String getAddressName() {
		return addressName;
	}
	public void setAddressName(String addressName) {
		this.addressName = addressName;
	}
	
}
