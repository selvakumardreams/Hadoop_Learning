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
 * Map Side Join
 * @author m1013673
 *
 */
public class MapSideJoin extends Configured implements Tool
{
    public static void main( String[] args ) throws Exception {
    	int exitCode = ToolRunner.run(new Configuration(),new MapSideJoin(), args);
    	System.exit(exitCode);
	}
    @Override
    public int run(String[] args) throws Exception {
    	Configuration conf = new Configuration();
    	
    	Job job = Job.getInstance(conf, "Map Side Join");
    	job.setJobName("Map Side Join");
    	job.setJarByClass(MapSideJoin.class);
    	job.addCacheFile(new URI("/inputs/mr_inputs/module5_ex1_f1#module5_ex1_f1"));
    	FileInputFormat.addInputPath(job,new Path(args[0]) );
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
    	job.setMapperClass(MapSideJoinMapper.class);
    	job.setNumReduceTasks(0);
    	
    	boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
    	
    }
}