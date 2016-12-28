package com.dreams.hadoop;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Store Map Side Join
 * @author Selva
 *
 */
public class StoreMapSideJoin extends Configured implements Tool {

	public static void main( String[] args ) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(),new StoreMapSideJoin(), args);
		System.exit(exitCode);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "Store Map Side Join");
		job.setJobName("Store Map Side Join");
		job.setJarByClass(StoreMapSideJoin.class);
		job.addCacheFile(new URI("/inputs/mr_inputs/store_details#store_details"));
		FileInputFormat.addInputPath(job,new Path(args[0]) );
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(StoreMapSideJoinMapper.class);
		job.setNumReduceTasks(0);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;

	}
}