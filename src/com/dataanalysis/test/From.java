package com.dataanalysis.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.dataanalysis.hdfs.HdfsDAO;

public class From {

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

	public static class FromMapper extends MapReduceBase implements
			Mapper<Object, Text, Text, Text> {
//		private final static NullWritable myNull = NullWritable.get();

		@Override
		public void map(Object key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			List<String> tokens = new ArrayList<String>(Arrays.asList(value
					.toString().split("	")));
			ArrayList<String> fromList = new ArrayList<String>();
			fromList.add(tokens.get(0));
			fromList.add(tokens.get(5));
			fromList.add(tokens.get(6));
			fromList.add(tokens.get(7));

			output.collect(new Text("from"), new Text(ListToString(fromList)));

		}
	}

	public static void main(String[] args) throws Exception {
		String input = "hdfs://192.168.1.206:9000/user/flp/data_result_from";
		String output = "hdfs://192.168.1.206:9000/user/flp/data_from";

		JobConf conf = new JobConf(Analysis.class);
		conf.setJobName("from");

		HdfsDAO hdfs = new HdfsDAO("hdfs://192.168.1.206:9000", conf);
		hdfs.rmr(output);

		conf.setMapperClass(FromMapper.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));

		JobClient.runJob(conf);
		System.exit(0);
	}

}
