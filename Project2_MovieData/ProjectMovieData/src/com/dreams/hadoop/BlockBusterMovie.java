package com.dreams.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.dreams.hadoop.util.TextPair;
/**
 * Block buster movie of each decade.
 * @author Selva
 *
 */
public class BlockBusterMovie {

	public static void main(String[] args) throws Exception	{

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "BlockBusterMovie");
		job.setJarByClass(BlockBusterMovie.class); 
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(MovieDataMapper.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(TextPair.class); 	    


		job.setNumReduceTasks(4);
		job.setReducerClass(MovieDataReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));		    

		FileOutputFormat.setOutputPath(job, new Path(args[1]));    

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
