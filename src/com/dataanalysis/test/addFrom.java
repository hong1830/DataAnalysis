package com.dataanalysis.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Reducer;


import com.dataanalysis.hdfs.HdfsDAO;
import com.sun.jndi.cosnaming.IiopUrl.Address;

public class addFrom {

	public static String ListToString(List<String> stringList) {

		StringBuffer buffer = new StringBuffer();
		boolean flag = false;
		for (String string : stringList) {
			if (flag) {
				buffer.append("	");
			} else {
				flag = true;
			}
			buffer.append(string);
		}
		return buffer.toString();
	}

	public static class AddFromMapper extends Mapper<Object, Text, Text, Text> {

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			List<String> tokens = new ArrayList<String>(Arrays.asList(value
					.toString().split("	")));
			ArrayList<String> list = new ArrayList<String>();
			list.add(tokens.get(2));
			list.add(tokens.get(3));
			list.add(tokens.get(4));
			context.write(new Text(ListToString(list)), new Text(
					(tokens.get(1)).toString() + "	"
							+ tokens.get(0).toLowerCase()));

		}

	}

	public static class AddFromReducer extends Reducer<Text, Text, FloatWritable, Text> {

		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			ArrayList<String> fromList = new ArrayList<String>();
			ArrayList<String> toList = new ArrayList<String>();
			String[] values_split = null;
			while (values.iterator().hasNext()) {
				values_split = values.iterator().next().toString().split("	");
				if (values_split[1].equalsIgnoreCase("from")) {
					fromList.add(values_split[0]);
				} else {
					toList.add(values_split[0]);
				}
			}
			if (fromList.size() != 0) {
				for (String string : fromList) {
					context.write(new FloatWritable(Float.valueOf(string)), new Text(
							ListToString(toList)));

				}
			}

		}
	}
	

	public static void main(String[] args) throws Exception {
		String input1 = "hdfs://namenode:9000/user/flp/data_to";
		String input2 = "hdfs://namenode:9000/user/flp/data_from";
		String output = "hdfs://namenode:9000/user/flp/data_addfrom";

		Configuration conf = new Configuration();

		Job job = new Job(conf, "addfrom");
		job.setJarByClass(addFrom.class);

		HdfsDAO hdfs = new HdfsDAO("hdfs://192.168.1.206:9000", conf);
		hdfs.rmr(output);
		
		

		job.setMapperClass(AddFromMapper.class);
		job.setReducerClass(AddFromReducer.class);
		job.setOutputKeyClass(FloatWritable.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(input1), new Path(input2));
		FileOutputFormat.setOutputPath(job, new Path(output));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		job.setOutputFormatClass(TextOutputFormat.class);

	}
}
