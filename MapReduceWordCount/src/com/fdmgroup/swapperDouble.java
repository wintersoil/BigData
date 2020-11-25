package com.fdmgroup;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class swapperDouble {

		public static class Map2 extends MapReduceBase implements Mapper<LongWritable, Text, DoubleWritable, Text> {

			 public void map(LongWritable key, Text value, OutputCollector<DoubleWritable, Text> output, Reporter reporter) throws IOException{
				 String[] kv = value.toString().split("\t");
				 String word = kv[0];
				 String freq = kv[1];
				 Double i = Double.parseDouble(freq);
				 output.collect(new DoubleWritable(i), new Text(word));
				 
			 }
		}
		
		public static class Reduce2 extends MapReduceBase implements Reducer<DoubleWritable, Text, DoubleWritable, Text> {
			
			public void reduce(DoubleWritable key, Iterator<Text> values, OutputCollector<DoubleWritable, Text> output, Reporter reporter) throws IOException {
				String accumulation = "";
				while (values.hasNext()) {
					accumulation += ", " + values.next();
				}
				output.collect(key, new Text(accumulation));
			}
		}

}
