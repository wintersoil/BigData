package com.fdmgroup;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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


import com.fdmgroup.swapperDouble.Map2;
import com.fdmgroup.swapperDouble.Reduce2;

public class fifa {
	
	public static class Map extends MapReduceBase implements Mapper <LongWritable, Text, Text, DoubleWritable>{
		private Text word;
		public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException
		{
			//,ID,Name,Age,Photo,Nationality,Flag,Overall,Potential,Club,Club Logo,Value,Wage,Special,Preferred Foot,International Reputation,Weak Foot,Skill Moves,Work Rate,Body Type,Real Face,Position,Jersey Number,Joined,Loaned From,Contract Valid Until,Height,Weight,LS,ST,RS,LW,LF,CF,RF,RW,LAM,CAM,RAM,LM,LCM,CM,RCM,RM,LWB,LDM,CDM,RDM,RWB,LB,LCB,CB,RCB,RB,Crossing,Finishing,HeadingAccuracy,ShortPassing,Volleys,Dribbling,Curve,FKAccuracy,LongPassing,BallControl,Acceleration,SprintSpeed,Agility,Reactions,Balance,ShotPower,Jumping,Stamina,Strength,LongShots,Aggression,Interceptions,Positioning,Vision,Penalties,Composure,Marking,StandingTackle,SlidingTackle,GKDiving,GKHandling,GKKicking,GKPositioning,GKReflexes,Release Clause
				String beg = value.toString();
				String[] params = beg.split(",");
				if(params[11].substring(1, params[11].length()-1).length() > 0)
				{
					if(params[11].substring(params[11].length()-1).equals("K"))
						output.collect(new Text(params[5] + " -> Valuation at : " + params[11].substring(0, 1) + " [units] -> K"), new DoubleWritable(Double.parseDouble(params[11].substring(1, params[11].length()-1))));
					else if(params[11].substring(params[11].length()-1).equals("M"))
						output.collect(new Text(params[5] + " -> Valuation at : " + params[11].substring(0, 1) + " [units] -> K"), new DoubleWritable(Double.parseDouble(params[11].substring(1, params[11].length()-1)) * 1000));
				}
					else
					output.collect(new Text(params[5] + " -> Valuation at : $ [units] -> M"), new DoubleWritable(0.00));
		}
	}
	
	public static class Reduce extends MapReduceBase implements Reducer <Text, DoubleWritable, Text, DoubleWritable>{
		public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
			Double valuation = 0.0;
			while(values.hasNext()) {
				valuation+= Double.parseDouble(values.next().toString());
			}
			output.collect(key, new DoubleWritable(valuation));
		}
	}
	
	public static void main(String[] args) throws IOException
	{
		JobConf conf = new JobConf(fifa.class);
		Job j = Job.getInstance(conf, "fifaValuations");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);
		
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
		
		JobConf conf2 = new JobConf(swapperDouble.class);
		Job j2 = Job.getInstance(conf2, "swapperDouble");
		
		conf2.setOutputKeyClass(DoubleWritable.class);
		conf2.setOutputValueClass(Text.class);
		
		conf2.setMapperClass((Class<? extends Mapper>) Map2.class);
		conf2.setCombinerClass((Class<? extends Reducer>) Reduce2.class);
		conf2.setReducerClass((Class<? extends Reducer>) Reduce2.class);
		
		conf2.setInputFormat(TextInputFormat.class);
		conf2.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf2, new Path(args[1]));
		FileOutputFormat.setOutputPath(conf2, new Path(args[2]));
		
		JobClient.runJob(conf2);
	}
	
	// On this data set:
	// https://www.kaggle.com/karangadiya/fifa19
	
	// Processed data
	// soccer-nationalities-gross-value-of-players
}
