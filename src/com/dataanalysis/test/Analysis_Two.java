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

public class Analysis_Two {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static class AnalysisMapper extends MapReduceBase implements
			Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(Object key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			List<String> tokens = new ArrayList<String>(Arrays.asList(value
					.toString().split("	")));
			int sum = 0;
		
			for (int i = 1; i < tokens.size(); i++) {
				if (Float.parseFloat(tokens.get(i)) != 0) {
					sum++;
				}
			}
			word.set("daughter num:"+String.valueOf(sum));
			output.collect(word, one);
		}
	}

	public static class AnalysisReducer extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			result.set(sum);
			output.collect(key, result);

		}

	}

	public static void main(String[] args) throws IOException {
		String input = "hdfs://namenode:9000/user/flp/"+ args[0];
		String output = "hdfs://namenode:9000/user/flp/" +args[1];

		JobConf conf = new JobConf(Analysis.class);
		conf.setJobName("Analysis_Two");

		HdfsDAO hdfs = new HdfsDAO("hdfs://192.168.1.206:9000", conf);
		hdfs.rmr(output);

		conf.setMapperClass(AnalysisMapper.class);
		conf.setCombinerClass(AnalysisReducer.class);
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
