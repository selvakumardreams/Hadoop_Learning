package com.dreams.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Module3Ex1
 * @author Selva
 *
 */
public class Module3Ex1 {

	private static final double COLUMN_VALUE = 20.0;
	private static final String TAG = "Module3Ex1";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		try {
			Job job = Job.getInstance(conf, "Module 3");
			job.setJarByClass(Module3Ex1.class);
			job.setMapperClass(Module3Ex1Mapper.class);
			job.setReducerClass(Module3Ex1Reducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			System.exit(job.waitForCompletion(true) ? 1 : 0);
		} catch (IOException e) {

		}
	}

	public static class Module3Ex1Mapper extends
	Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] arr = line.split("\\t|\\s+");
			double column3 = 0;
			String finalRes = "";

			try {
				column3 = Double.parseDouble(arr[2]);

				System.out.println(TAG + "column3 : " + column3);

				if (column3 <= COLUMN_VALUE) {
					System.out.println(TAG + "Condition : match");
					finalRes = arr[0] + "\t" + arr[1] + "\t" + column3;
					System.out.println(TAG + "finalRes : " + finalRes);
					context.write(new Text(arr[0]), new Text(finalRes));
				} else {
					System.out.println(TAG + "Condition : not match");
				}

			} catch (ArrayIndexOutOfBoundsException e) {
				e.printStackTrace();
			}


		}
	}

	public static class Module3Ex1Reducer extends 
	Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			String finalRes = "";
			
			for (Text x : value) {
				String[] temp = x.toString().split("\t");
				System.out.println(TAG + "Reducer : " + temp);
				finalRes =  temp[1] + "\t" + temp[2];
				context.write(new Text(temp[0]), new Text(finalRes));
			}
		}
	}

}