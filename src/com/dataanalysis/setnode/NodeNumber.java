package com.dataanalysis.setnode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.dataanalysis.hdfs.HdfsDAO;

public class NodeNumber {

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

	public static class NMap extends Mapper<Object, Text, Text, NullWritable> {

		Text keyOut = new Text();
		NullWritable mynull = NullWritable.get();
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			List<String> tokens = new ArrayList<String>(Arrays.asList(value
					.toString().split("	")));
			List<String> sNode = tokens.subList(6, 9);
			List<String> eNode = tokens.subList(9, 12);
			keyOut.set(ListToString(sNode));
			context.write(keyOut, mynull);
			keyOut.set(ListToString(eNode));
			context.write(keyOut, mynull);

		}
	}

	public static class NReduce extends Reducer<Text, Text, Text, Text> {
		
		IntWritable num = new IntWritable(0);

		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			num.set(num.get()+1);
			context.write(key, new Text("@"+num.toString()));
		}

		public static void main(String[] args) throws Exception {

			String input = "hdfs://namenode:9000/user/flp/" + args[0];
			String output = "hdfs://namenode:9000/user/flp/" + args[1];

			Configuration conf = new Configuration();

			Job job = new Job(conf, "NodeNumber");
			job.setJarByClass(NodeNumber.class);

			HdfsDAO hdfs = new HdfsDAO("hdfs://192.168.1.206:9000", conf);
			hdfs.rmr(output);

			job.setMapperClass(NMap.class);
			job.setReducerClass(NReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(NullWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			FileInputFormat.setInputPaths(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));

			job.waitForCompletion(true);

		}

	}

}
