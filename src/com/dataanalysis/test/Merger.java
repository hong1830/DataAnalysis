package com.dataanalysis.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.dataanalysis.hdfs.HdfsDAO;

public class Merger {

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



	public static class MergerMap extends Mapper<Object, Text, Text, Text> {

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			List<String> tokens = new ArrayList<String>(Arrays.asList(value
					.toString().split("	")));
			context.write(new Text(tokens.get(0)), new Text(tokens.get(1)));
		}
	}

	public static class MergerReduce extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			List<String> result = new ArrayList<String>();
			for (; values.iterator().hasNext();) {
				result.add(values.iterator().next().toString());
			}
			switch (result.size()) {
			case 1:
				context.write(key, new Text(ListToString(result)+"	0.0"+"	0.0"+"	0.0"));
				break;
			case 2:
				context.write(key, new Text(ListToString(result)+"	0.0"+"	0.0"));
				break;
			case 3:
				context.write(key, new Text(ListToString(result)+"	0.0"));
				break;
			case 4:
				context.write(key, new Text(ListToString(result)));
				break;
			}
		}

	}

	public static void main(String[] args) throws Exception {
		String input = "hdfs://namenode:9000/user/flp/" + args[0];
		String output = "hdfs://namenode:9000/user/flp/" + args[1];

		Configuration conf = new Configuration();

		Job job = new Job(conf, "Merger");
		job.setJarByClass(addFrom.class);

		HdfsDAO hdfs = new HdfsDAO("hdfs://192.168.1.206:9000", conf);
		hdfs.rmr(output);

		job.setMapperClass(MergerMap.class);
		job.setReducerClass(MergerReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		job.setOutputFormatClass(TextOutputFormat.class);
	}

}
