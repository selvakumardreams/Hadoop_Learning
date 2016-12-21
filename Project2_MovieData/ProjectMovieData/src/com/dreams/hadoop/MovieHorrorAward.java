package com.dreams.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * MovieHorrorAward
 * @author selva
 * The number of awards won by movies under the “Horror” genre released between 1952
 * to 1968.
 */
public class MovieHorrorAward {

	public static void main(String[] args) throws Exception	{

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "MovieHorrorAward");
		job.setJarByClass(MovieHorrorAward.class); 
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(MovieHorrorAwardMapper.class);

		job.setNumReduceTasks(1);
		job.setReducerClass(MovieHorrorAwardReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));		    

		FileOutputFormat.setOutputPath(job, new Path(args[1]));    

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
