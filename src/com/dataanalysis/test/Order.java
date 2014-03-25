package com.dataanalysis.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.dataanalysis.hdfs.HdfsDAO;

public class Order {

	public static class OrderMap extends
			Mapper<Object, Text, FloatWritable, IntWritable> {

		private final static IntWritable one = new IntWritable(1);

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			List<String> tokens = new ArrayList<String>(Arrays.asList(value
					.toString().split("	")));
			FloatWritable segment = new FloatWritable(Float.valueOf(tokens
					.get(2)));
			tokens.remove(0);
			context.write(segment, one);

		}

	}

	public static class OrderReduce extends
			Reducer<FloatWritable, IntWritable, FloatWritable, IntWritable> {
		private IntWritable result = new IntWritable();

		protected void reduce(FloatWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			while (values.iterator().hasNext()) {
				sum += values.iterator().next().get();
			}
			result.set(sum);
			context.write(key, result);
		}

	}

	public static void main(String[] args) throws Exception {
		String input = "hdfs://namenode:9000/user/flp/" + args[0];
		String output = "hdfs://namenode:9000/user/flp/" + args[1];

		Configuration conf = new Configuration();

		Job job = new Job(conf, "Order");
		job.setJarByClass(Order.class);

		HdfsDAO hdfs = new HdfsDAO("hdfs://192.168.1.206:9000", conf);
		hdfs.rmr(output);

		job.setMapperClass(OrderMap.class);
		job.setReducerClass(OrderReduce.class);
		job.setMapOutputKeyClass(FloatWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(FloatWritable.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.waitForCompletion(true);

	}

}
