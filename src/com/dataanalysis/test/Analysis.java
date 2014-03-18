package com.dataanalysis.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
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

	public static class AnalysisMapper extends MapReduceBase implements
			Mapper<Object, Text, NullWritable, Text> {
		private final static NullWritable myNull = NullWritable.get();
		private final static int[] indexArr = new int[] { 4, 10, 14, 15, 16,
				17, 18, 23, 24, 25 };

		@Override
		public void map(Object key, Text value,
				OutputCollector<NullWritable, Text> output, Reporter reporter)
				throws IOException {
			List<String> tokens = new ArrayList<String>(Arrays.asList(value
					.toString().split("	")));
			if (Float.parseFloat(tokens.get(0).toString()) != 0) {
				String end = tokens.get(24).toString();

				for (int i = indexArr.length - 1; i >= 0; i--) {
					if (i <= tokens.size()) {
						tokens.remove(indexArr[i] - 1);
					}
				}
				tokens.add(end);
				output.collect(myNull, new Text(ListToString(tokens)));
			}

		}
	}

	public static void main(String[] args) throws Exception {
		String input = "hdfs://192.168.1.206:9000/user/flp/datatest";
		String output = "hdfs://192.168.1.206:9000/user/flp/data_result";

		JobConf conf = new JobConf(Analysis.class);
		conf.setJobName(" Analysis");

		HdfsDAO hdfs = new HdfsDAO("hdfs://192.168.1.206:9000", conf);
		hdfs.rmr(output);

		conf.setMapperClass(AnalysisMapper.class);

		conf.setOutputKeyClass(NullWritable.class);
		conf.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));

		JobClient.runJob(conf);
		System.exit(0);
	}

}
