package com.dataanalysis.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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

import com.dataanalysis.hdfs.HdfsDAO;

public class NoDaughter {
	
	public static class AnalysisMapper extends MapReduceBase implements
			Mapper<Object, Text, NullWritable, Text> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private Integer d_Num = 0;

		@Override
		public void map(Object key, Text value,
				OutputCollector<NullWritable, Text> output, Reporter reporter)
				throws IOException {

			List<String> tokens = new ArrayList<String>(Arrays.asList(value
					.toString().split("	")));
			int sum = 0;
			for (int i = 0; i < 4; i++) {
				if (Float.parseFloat(tokens.get(11 + i)) != 0) {
					sum++;
				}
			}
			if (sum == d_Num) {
				output.collect(NullWritable.get(), value);
			}
	
		}

		@Override
		public void configure(JobConf job) {
			// TODO Auto-generated method stub
			super.configure(job);
			JobConf conf = job;
			d_Num = Integer.valueOf(conf.get("d_num"));
			
		}
	}

//	public static class AnalysisReducer extends MapReduceBase implements
//			Reducer<Text, IntWritable, NullWritable, Text> {
//		private IntWritable result = new IntWritable();
//
//		@Override
//		public void reduce(Text key, Iterator<IntWritable> values,
//				OutputCollector<NullWritable, Text> output, Reporter reporter)
//				throws IOException {
//			int sum = 0;
//			while (values.hasNext()) {
//				sum += values.next().get();
//			}
//			result.set(sum);
//			output.collect(NullWritable.get(), result);
//
//		}
//
//	}

	public static void main(String[] args) throws IOException {
		String input = "hdfs://namenode:9000/user/flp/data";
		String output = "hdfs://namenode:9000/user/flp/data_daughter";
		
		Integer d_Num = Integer.valueOf(args[0]);
		
		

		JobConf conf = new JobConf(Analysis.class);
		conf.setJobName("Analysis_Two");
		

		HdfsDAO hdfs = new HdfsDAO("hdfs://192.168.1.206:9000", conf);
		hdfs.rmr(output);
		conf.set("d_num", args[0]);

		conf.setMapperClass(AnalysisMapper.class);
//		conf.setCombinerClass(AnalysisReducer.class);
//		conf.setReducerClass(AnalysisReducer.class);
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
