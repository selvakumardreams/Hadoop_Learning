package com.dreams.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * CounterRecords
 * @author Selva
 *
 */
public class CounterRecords extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		 int exitCode = ToolRunner.run(new CounterRecords(), args);
		 System.exit(exitCode);
	}

	public static class CounterRecordsMapper 
	extends MapReduceBase
	implements Mapper<LongWritable, Text, Text, IntWritable> {

		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
						throws IOException {

			String[] tokens = value.toString().split("\\t|\\s+");

			System.out.println("Counter: " + tokens[0] + ":" + tokens[1] + ":" + tokens[2]);
			String age = tokens[1].toString();
			int ageInt = Integer.parseInt(age);
			System.out.println("Counter: " + ageInt);

			if (ageInt <= 10) {
				reporter.getCounter("MAP_RECORD_COUNTER", "<=10").increment(1);
			} else if (ageInt >= 50) {
				reporter.getCounter("MAP_RECORD_COUNTER", ">=50").increment(1);
			} else {
				reporter.getCounter("MAP_RECORD_COUNTER", "NONE").increment(1);
			}
			output.collect(new Text(age), new IntWritable(1));
		}

	}

	@Override
	public int run(String[] args) throws Exception {
		String input, output;
		if(args.length == 2) {
			input = args[0];
			output = args[1];
		} else {
			input = "your-input-dir";
			output = "your-output-dir";
		}
		JobConf conf = new JobConf(getConf(), CounterRecords.class);
		conf.setJobName(this.getClass().getName());
		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));
		conf.setMapperClass(CounterRecordsMapper.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setNumReduceTasks(0);
		RunningJob job = JobClient.runJob(conf);
		long lessthan10 = job.getCounters().findCounter("MAP_RECORD_COUNTER","<=10").getValue();
		long greaterthan50 = job.getCounters().findCounter("MAP_RECORD_COUNTER",">=50").getValue();
		System.out.println("Less than 10: " + lessthan10);
		System.out.println("Greater than 50: " + greaterthan50); 
		return 0;
	}
}
