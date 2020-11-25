package com.fdmgroup;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
import org.apache.hadoop.mapreduce.Job;

import com.fdmgroup.wc.Map;
import com.fdmgroup.wc.Reduce;

public class averagelength {
	
	public static class Map extends MapReduceBase implements Mapper <LongWritable, Text, Text, IntWritable>{
		private Text word;
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
		{
				String beg = value.toString();
				Integer lengthy = 0;
				StringTokenizer tokenizer = new StringTokenizer(beg);
				while(tokenizer.hasMoreTokens())
				{
					beg = tokenizer.nextToken();
					lengthy = beg.length();
					beg = beg.substring(0, 1);
					output.collect(new Text(beg), new IntWritable(lengthy));
				}
		}
	}
	
	public static class Reduce extends MapReduceBase implements Reducer <Text, IntWritable, Text, DoubleWritable>{
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
			Double average = 0.0;
			Integer counter = 0;
			while(values.hasNext()) {
				average += (1.0 * values.next().get());
				counter++;
			}
			average = average / counter;
			output.collect(key, new DoubleWritable(average));
		}
	}
	
	public static void main(String[] args) throws Exception{
		JobConf conf = new JobConf(averagelength.class);
		Job j = Job.getInstance(conf, "lengths");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
	}
	

}
