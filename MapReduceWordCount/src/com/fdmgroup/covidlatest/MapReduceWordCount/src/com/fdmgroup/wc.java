package com.fdmgroup;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
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

import com.fdmgroup.wc1.Map1;
import com.fdmgroup.wc1.Reduce1;

public class wc {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		 private final static IntWritable one = new IntWritable(1);
		 private Text word = new Text();
		 
		 
		 public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException{
			 String line = value.toString();
			 StringTokenizer tokenizer = new StringTokenizer(line);
			 while(tokenizer.hasMoreTokens()) {
				 word.set(tokenizer.nextToken());
				 String st = word.toString();
				 st = st.replaceAll("[?*\\[\\],\\.!:\\-\';]", "");
				 word = new Text(st);
				 output.collect(word, one);
			 }
		 }
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}
	
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(wc.class);
		Job j = Job.getInstance(conf, "wordcount");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
		
		JobConf conf2 = new JobConf(wc1.class);
		Job j2 = Job.getInstance(conf2, "swapper");
		
		conf2.setOutputKeyClass(IntWritable.class);
		conf2.setOutputValueClass(Text.class);
		
		conf2.setMapperClass((Class<? extends Mapper>) Map1.class);
		conf2.setCombinerClass((Class<? extends Reducer>) Reduce1.class);
		conf2.setReducerClass((Class<? extends Reducer>) Reduce1.class);
		
		conf2.setInputFormat(TextInputFormat.class);
		conf2.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf2, new Path(args[1]));
		FileOutputFormat.setOutputPath(conf2, new Path(args[2]));
		
		JobClient.runJob(conf2);
		
	}

}
 