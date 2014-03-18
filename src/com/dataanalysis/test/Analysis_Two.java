package com.dataanalysis.test;

import java.io.IOException;
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

public class Analysis_Two {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static class AnalysisMapper extends MapReduceBase implements
			Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		

		@Override
		public void map(Object key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
				String[] tokens = value.toString().split("	");
				

		}
	}

	public static class AnalysisReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text arg0, Iterator<IntWritable> arg1,
				OutputCollector<Text, IntWritable> arg2, Reporter arg3)
				throws IOException {
			// TODO Auto-generated method stub
			
		}

	}

	public static void main(String[] args) throws IOException {
		String input = "hdfs://namenode:9000/user/flp/data";
		String output = "hdfs://namenode:9000/user/flp/data_result";

		JobConf conf = new JobConf(Analysis.class);
		conf.setJobName("WordCount");

		HdfsDAO hdfs = new HdfsDAO("hdfs://192.168.1.206:9000", conf);
		hdfs.rmr(output);

		conf.setMapperClass(AnalysisMapper.class);
		conf.setReducerClass(AnalysisReducer.class);
		// conf.setCombinerClass(WordCountReducer.class);
		// conf.setReducerClass(WordCountReducer.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));

		JobClient.runJob(conf);
		System.exit(0);

	}

}
