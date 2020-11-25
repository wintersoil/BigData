package com.fdmgroup;

import java.io.IOException;
import java.util.Iterator;

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

import com.fdmgroup.covid.Map;
import com.fdmgroup.covid.Reduce;
import com.fdmgroup.wc1.Map1;
import com.fdmgroup.wc1.Reduce1;

public class morganstanley {
	public static class Map extends MapReduceBase implements Mapper <LongWritable, Text, DoubleWritable, Text>{
		private Text word;
		public void map(LongWritable key, Text value, OutputCollector<DoubleWritable, Text> output, Reporter reporter) throws IOException
		{
			// time, open, high, low, last, change, volume
				String beg = value.toString();
				String[] params = beg.split(",");
			//	output.collect(new DoubleWritable(Double.parseDouble(params[2])), new IntWritable(1)); // 
				output.collect(new DoubleWritable(Double.parseDouble(params[2])), new Text(params[0])); // 

		}
	}
	
	public static class Reduce extends MapReduceBase implements Reducer <DoubleWritable, Text, DoubleWritable, Text>{
		public void reduce(DoubleWritable key, Iterator<Text> values, OutputCollector<DoubleWritable, Text> output, Reporter reporter) throws IOException {
			String accumulation = "";
			while(values.hasNext()) {
				accumulation += values.next();
			}
			output.collect(key, new Text(accumulation));
		}
	}
	
	public static void main(String[] args) throws Exception{
		JobConf conf = new JobConf(morganstanley.class);
		Job j = Job.getInstance(conf, "stocks");
		
		conf.setOutputKeyClass(DoubleWritable.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
		
//		JobConf conf2 = new JobConf(wc1.class);
//		Job j2 = Job.getInstance(conf2, "swapper");
//		
//		conf2.setOutputKeyClass(IntWritable.class);
//		conf2.setOutputValueClass(Text.class);
//		
//		conf2.setMapperClass((Class<? extends Mapper>) Map1.class);
//		conf2.setCombinerClass((Class<? extends Reducer>) Reduce1.class);
//		conf2.setReducerClass((Class<? extends Reducer>) Reduce1.class);
//		
//		conf2.setInputFormat(TextInputFormat.class);
//		conf2.setOutputFormat(TextOutputFormat.class);
//		
//		FileInputFormat.setInputPaths(conf2, new Path(args[1]));
//		FileOutputFormat.setOutputPath(conf2, new Path(args[2]));
//		
//		JobClient.runJob(conf2);
	}
		
		// Big Data from :  https://www.barchart.com/stocks/quotes/MS/price-history/historical#
		//  https://i.ibb.co/xGtmkS0/morgan-stanley-stock-highs-sorted-last-2-years.png
		// check morgan-stanley-stocks-sorted for processed data
}
