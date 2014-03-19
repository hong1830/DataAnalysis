package com.dataanalysis.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.dataanalysis.hdfs.HdfsDAO;

public class addFrom {
	public static class AddFromMapper extends MapReduceBase implements
			Mapper<Object, Text, Text, Text> {
		
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

		@Override
		public void map(Object key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			List<String> tokens = new ArrayList<String>(Arrays.asList(value
					.toString().split("	")));
			ArrayList<String> list = new ArrayList<String>();
			list.add(tokens.get(2));
			list.add(tokens.get(3));
			list.add(tokens.get(4));
			output.collect(new Text(ListToString(list)), new Text((tokens.get(1)).toString()+"	"+tokens.get(0).toLowerCase()));

		}
	}

	public static class AddFromReducer extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

		}

	}

	public static void main(String[] args) throws IOException {
		String input1 = "hdfs://namenode:9000/user/flp/data_to";
		String input2 = "hdfs://namenode:9000/user/flp/data_from";
		String output = "hdfs://namenode:9000/user/flp/data_addfrom";

		JobConf conf = new JobConf(Analysis.class);
		conf.setJobName("addfrom");

		HdfsDAO hdfs = new HdfsDAO("hdfs://192.168.1.206:9000", conf);
		hdfs.rmr(output);

		conf.setMapperClass(AddFromMapper.class);
//		conf.setCombinerClass(AddFromReducer.class);
//		conf.setReducerClass(AddFromReducer.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(input1), new Path(input2));
		FileOutputFormat.setOutputPath(conf, new Path(output));

		JobClient.runJob(conf);
		System.exit(0);

	}
}
