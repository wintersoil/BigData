package com.fdmgroup.covidlatest;

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

import com.fdmgroup.wc1;
import com.fdmgroup.averagelength.Map;
import com.fdmgroup.averagelength.Reduce;
import com.fdmgroup.wc1.Map1;
import com.fdmgroup.wc1.Reduce1;

public class covidlatest {

		//Row_ID,Accurate_Episode_Date,Case_Reported_Date,Test_Reported_Date,Specimen_Date,Age_Group,Client_Gender,Case_AcquisitionInfo,Outcome1,Outbreak_Related,Reporting_PHU_ID,Reporting_PHU,Reporting_PHU_Address,Reporting_PHU_City,Reporting_PHU_Postal_Code,Reporting_PHU_Website,Reporting_PHU_Latitude,Reporting_PHU_Longitude


		
		public static class Map extends MapReduceBase implements Mapper <LongWritable, Text, Text, IntWritable>{
			private Text word;
			public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
			{
				//Row_ID,Accurate_Episode_Date,Case_Reported_Date,Test_Reported_Date,Specimen_Date,Age_Group,Client_Gender,Case_AcquisitionInfo,Outcome1,Outbreak_Related,Reporting_PHU_ID,Reporting_PHU,Reporting_PHU_Address,Reporting_PHU_City,Reporting_PHU_Postal_Code,Reporting_PHU_Website,Reporting_PHU_Latitude,Reporting_PHU_Longitude
					String beg = value.toString();
					if(beg.contains("\""))
					{
						String s1 = beg.substring(beg.indexOf("\"") + 1);
						s1 = s1.substring(0, s1.indexOf("\""));
						int lengthy = s1.length();
						s1 = s1.replaceAll(",", "");
						beg = beg.substring(0, beg.indexOf("\"")) + s1 + beg.substring(beg.indexOf("\"") + lengthy + 2);
					}
					String[] params = beg.split(",");
					output.collect(new Text(params[13]), new IntWritable(1)); // Gender count
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
			JobConf conf = new JobConf(covidlatest.class);
			Job j = Job.getInstance(conf, "covidBD");
			
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
		
			// On this data set:
			// https://data.ontario.ca/dataset/f4112442-bdc8-45d2-be3c-12efae72fb27/resource/455fd63b-603d-4608-8216-7d8647f43350/download/conposcovidloc.csv

			// Results:
			// covid-latest-ontario

}
