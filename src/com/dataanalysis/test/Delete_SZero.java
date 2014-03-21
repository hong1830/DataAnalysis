package com.dataanalysis.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.output.NullWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;

import com.dataanalysis.hdfs.HdfsDAO;

public class Delete_SZero {

	public static class DeleteMap extends
			Mapper<Object, Text, FloatWritable, Text> {

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			List<String> tokens = new ArrayList<String>(Arrays.asList(value
					.toString().split("	")));
			context.write(new FloatWritable(Float.valueOf(tokens.get(0))),
					new Text("legal"));
			for (int i = 1; i < 4; i++) {
				if (!(Float.valueOf(tokens.get(i)) == 0)) {
					context.write(
							new FloatWritable(Float.valueOf(tokens.get(i))),
							value);
				}
			}
		}
	}

	public static class DeleteReduce extends
			Reducer<FloatWritable, Text, FloatWritable, Text> {
		private NullWritable mynull = NullWritable.get();

		protected void reduce(FloatWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			boolean isExists = false;
			List<String> outputs = new ArrayList<String>();
			while (values.iterator().hasNext()) {
				String value = values.iterator().next().toString();
				System.out.println(value);
				if (value.equals("legal")) {
					isExists = true;
				} else {
					outputs.add(value);
				}
			}
			if (isExists) {
				context.write(new FloatWritable(Float.valueOf(key.toString())),
						new Text("0.0"));
				for (String output : outputs) {
					List<String> outputList = new ArrayList<String>(
							Arrays.asList(output.split("\t")));
					context.write(
							new FloatWritable(Float.valueOf(outputList.get(0))),
							new Text(key.toString()));
				}
			}
			// if (isExists)) {
			// for (String output : outputs) {
			// List<String> outputList = new ArrayList<String>(
			// Arrays.asList(output.split("\t")));
			// context.write(new Text(outputList.get(0)), new
			// Text(key.toString()+"0	0"));
			// }else{
			// context.write(new Text(key.toString()), new
			// Text(key.toString()+"0	0	0"));
			// }
			//
			// }

		}

	}

	public static void main(String[] args) throws Exception {
		// String input = "hdfs://namenode:9000/user/flp/data_sort_test";
		String input = "hdfs://namenode:9000/user/flp/data_sort_test";
		String output = "hdfs://namenode:9000/user/flp/data_delete_test";

		Configuration conf = new Configuration();

		Job job = new Job(conf, "delete");
		job.setJarByClass(addFrom.class);

		HdfsDAO hdfs = new HdfsDAO("hdfs://192.168.1.206:9000", conf);
		hdfs.rmr(output);

		job.setMapperClass(DeleteMap.class);
		job.setReducerClass(DeleteReduce.class);

		job.setMapOutputKeyClass(FloatWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		job.setOutputFormatClass(TextOutputFormat.class);
	}
}
