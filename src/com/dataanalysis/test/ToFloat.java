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

public class ToFloat {

	public static String ListToString(List<Float> floatList) {
		StringBuffer buffer = new StringBuffer();
		boolean flag = false;
		for (Float f : floatList) {
			if (flag) {
				buffer.append("	");
			} else {
				flag = true;
			}
			buffer.append(f.toString());
		}
		return buffer.toString();
	}

	public static class ToFloatMapper extends MapReduceBase implements
			Mapper<Object, Text, NullWritable, Text> {
		private final static NullWritable myNull = NullWritable.get();

		@Override
		public void map(Object key, Text value,
				OutputCollector<NullWritable, Text> output, Reporter reporter)
				throws IOException {
			List<String> tokens = new ArrayList<String>(Arrays.asList(value
					.toString().split("	")));
			List<Float> toFloat = new ArrayList<Float>();
			for(String str:tokens){
				toFloat.add(Float.valueOf(str));
			}
			output.collect(myNull, new Text(ListToString(toFloat)));
		}
	}

	public static void main(String[] args) throws Exception {
		String input = "hdfs://namenode:9000/user/flp/" + args[0];
		String output = "hdfs://namenode:9000/user/flp/" + args[1];

		JobConf conf = new JobConf(Analysis.class);
		conf.setJobName("ToFloat");

		HdfsDAO hdfs = new HdfsDAO("hdfs://192.168.1.206:9000", conf);
		hdfs.rmr(output);

		conf.setMapperClass(ToFloatMapper.class);

		conf.setOutputKeyClass(NullWritable.class);
		conf.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));

		JobClient.runJob(conf);
		System.exit(0);
	}

}
