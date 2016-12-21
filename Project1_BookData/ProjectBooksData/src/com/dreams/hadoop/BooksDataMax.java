package com.dreams.hadoop;

import java.io.IOException;
import java.util.regex.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Which year the maximum number of books were published
 * @author Selva
 *
 */
public class BooksDataMax {

	public static class Map extends
	Mapper<LongWritable, Text, Text, IntWritable> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();

			Pattern pattern = Pattern.compile("\"[0-9]{4}\"");
			Matcher match = pattern.matcher(line);
			if(match.find()){
				String year = match.group();
				year = year.replaceAll("\"", "");
				value.set(year);
				context.write(value, new IntWritable(1));
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{

		int max = 0;
		Text year = new Text("");

		public void reduce(Text key, Iterable<IntWritable> value, Context context){

			int sum = 0;
			for(IntWritable x : value){
				sum += x.get();
			}

			if(sum>max){
				max = sum;
				year.set(key);
			}
		}

		@Override
		protected void cleanup(Context context)throws IOException, InterruptedException{
			context.write(year, new IntWritable(max));
		}
	}

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Maximum number of books published in a year");

		job.setJarByClass(BooksDataMax.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		Path outputPath = new Path(args[1]);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		outputPath.getFileSystem(conf).delete(outputPath, true);

		System.exit(job.waitForCompletion(true)?0:1);
	}

}