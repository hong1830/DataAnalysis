package com.dataanalysis.test;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
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
import org.mockito.internal.matchers.Null;

import com.dataanalysis.hdfs.HdfsDAO;

public class Analysis {

	public static class AnalysisMapper extends MapReduceBase implements
			Mapper<Object, Text, NullWritable, Text> {
		private final static NullWritable myNull = NullWritable.get();

		@Override
		public void map(Object key, Text value,
				OutputCollector<NullWritable, Text> output, Reporter reporter)
				throws IOException {
			String[] tokens = value.toString().split("	");
			if (Float.valueOf(tokens[0]) != 0) {
				output.collect(myNull, value);
			}

		}
	}

//	public static class WordCountReducer extends MapReduceBase implements
//			Reducer<Text, IntWritable, Text, IntWritable> {
//		private IntWritable result = new IntWritable();
//
//		@Override
//		public void reduce(Text key, Iterator<IntWritable> values,
//				OutputCollector<Text, IntWritable> output, Reporter reporter)
//				throws IOException {
//			int sum = 0;
//			while (values.hasNext()) {
//				sum += values.next().get();
//			}
//			output.collect(key, result);
//		}
//	}

	public static void main(String[] args) throws Exception {
		String input = "hdfs://namenode:9000/user/flp/data";
		String output = "hdfs://namenode:9000/user/flp/data_result";

		JobConf conf = new JobConf(Analysis.class);
		conf.setJobName(" Analysis");

		HdfsDAO hdfs = new HdfsDAO("hdfs://192.168.1.206:9000", conf);
		hdfs.rmr(output);

		conf.setMapperClass( AnalysisMapper.class);
		// conf.setCombinerClass(WordCountReducer.class);
		// conf.setReducerClass(WordCountReducer.class);

		conf.setOutputKeyClass(NullWritable.class);
		conf.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));

		JobClient.runJob(conf);
		System.exit(0);
	}

}
