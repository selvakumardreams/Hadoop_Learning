package com.dreams.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.dreams.hadoop.util.DoublePair;
import com.dreams.hadoop.util.StableGroupComp;
import com.dreams.hadoop.util.StableJoinPart;
import com.dreams.hadoop.util.StableSortComp;
import com.dreams.hadoop.util.TextPair;

/**
 * MovieStableActor
 * @author Selva
 *
 */
public class MovieStableActor {
	
	public static void main(String[] args) throws Exception	{

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "MovieStableActor");
		job.setJarByClass(MovieStableActor.class); 
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(MovieStableActorMapper.class);

		job.setMapOutputKeyClass(DoublePair.class);
		job.setMapOutputValueClass(TextPair.class);
		job.setSortComparatorClass(StableSortComp.class);
		job.setGroupingComparatorClass(StableGroupComp.class);

		job.setNumReduceTasks(1);
		job.setPartitionerClass(StableJoinPart.class);
		job.setReducerClass(MovieStableActorReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));		    

		FileOutputFormat.setOutputPath(job, new Path(args[1]));    		    
		System.exit(job.waitForCompletion(true) ? 0 : 1);	 	     	    	 	    
	}
}
