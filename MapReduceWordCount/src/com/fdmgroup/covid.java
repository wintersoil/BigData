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

import com.fdmgroup.averagelength.Map;
import com.fdmgroup.averagelength.Reduce;
import com.fdmgroup.wc1.Map1;
import com.fdmgroup.wc1.Reduce1;

public class covid {
	
	public static class Map extends MapReduceBase implements Mapper <LongWritable, Text, Text, IntWritable>{
		private Text word;
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
		{
			//_id,Outbreak Associated,Age Group,Neighbourhood Name,FSA,Source of Infection,Classification,Episode Date,Reported Date,Client Gender,Outcome,Currently Hospitalized,Currently in ICU,Currently Intubated,Ever Hospitalized,Ever in ICU,Ever Intubated
				String beg = value.toString();
				String[] params = beg.split(",");
			//	output.collect(new Text(params[3]+ " : " + params[4]), new IntWritable(1)); // Neighborhood, FPA
			//	output.collect(new Text(params[3]+ " : " + params[4] + " >> "+ params[10]), new IntWritable(1)); // Neighborhood, FPA , Gender;
			//	output.collect(new Text(params[10]), new IntWritable(1)); // Fatal, Resolved, Active 
				output.collect(new Text(params[9] + " with status => " + params[10]), new IntWritable(1)); // Male, Female, Other and their status
		}
	}
	
	public static class Reduce extends MapReduceBase implements Reducer <Text, IntWritable, Text, IntWritable>{
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			Integer counter = 0;
			while(values.hasNext()) {
				counter+= values.next().get();
			}
			output.collect(key, new IntWritable(counter));
		}
	}
	
	public static void main(String[] args) throws Exception{
		JobConf conf = new JobConf(covid.class);
		Job j = Job.getInstance(conf, "confirmed");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setMapperClass(Map.class);
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
