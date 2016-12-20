package com.dreams.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * @author Selva
 *
 */
public class RemoveLengthRecord {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		try {
			Job job = Job.getInstance(conf, "Remove Record length less than 15");
			job.setMapperClass(RecordMapper.class);
			job.setReducerClass(RecordReducer.class);
			job.setJarByClass(RecordReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(NullWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			System.exit(job.waitForCompletion(true) ? 1 : 0);
		} catch (IOException e) {

		}
	}
}