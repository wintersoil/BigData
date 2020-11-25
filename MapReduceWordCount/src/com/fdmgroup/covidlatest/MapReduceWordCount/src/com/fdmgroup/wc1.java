package com.fdmgroup;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class wc1{	
	public static class Map1 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {

		 public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException{
			 String[] kv = value.toString().split("\t");
			 String word = kv[0];
			 String freq = kv[1];
			 Integer i = Integer.valueOf(freq);
			 output.collect(new IntWritable(i), new Text(word));
			 
		 }
	}
	
	public static class Reduce1 extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
		
		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			String accumulation = "";
			while (values.hasNext()) {
				accumulation += ", " + values.next();
			}
			output.collect(key, new Text(accumulation));
		}
	}
}