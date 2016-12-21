package com.dreams.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.dreams.hadoop.util.IntPair;

/**
 * The top 3 most promising pair of actor and actress
 * @author Selva
 *
 */
public class ActorActressPair {
	
	public static void main(String[] args) throws Exception	{

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ActorActressPair");
		job.setJarByClass(ActorActressPair.class); 
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(ActorActressPairMapper.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntPair.class); 	    


		job.setNumReduceTasks(1);
		job.setReducerClass(ActorActressPairReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));		    

		FileOutputFormat.setOutputPath(job, new Path(args[1]));    

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
