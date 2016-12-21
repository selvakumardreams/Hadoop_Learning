package com.dreams.hadoop;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 
 * @author Selva
 *
 */
public class BooksDataRanks {
	
	public static class BookJoinMap extends
	Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] arr = line.split(";");
			String isbn = "";
			String year = "";

			Pattern pattern = Pattern.compile("\"[0-9]{4}\"");
			Matcher match = pattern.matcher(line);

			if (match.find()) {
				year = match.group();
			}
			if (!year.equals("") && !arr[0].equals("ISBN")) {
				isbn = arr[0];
				context.write(new Text(isbn), new Text("Year\t" + year));
			}
		}
	}

	public static class BooksDataRankJoinMap extends
	Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] arr = line.split(";");
			if (!arr[1].equals("ISBN") && !arr[2].equals("Book-Rating")) {
				String isbn = arr[1];
				String rank = arr[2];
				context.write(new Text(isbn), new Text("Rank\t" + rank));
			}
		}
	}

	public static class JoinReduce extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			String year = "";
			String rank = "";
			String finalRes = "";
			float avgRank = 0;
			int nRanks = 0;

			for (Text x : value) {
				String[] temp = x.toString().split("\t");

				if (temp.length >= 2) {
					if (temp[0].equals("Year")
							&& !temp[1].equals("Year-Of-Publication")) {
						year = temp[1];
					}
					if (temp[0].equals("Rank")
							&& !temp[1].equals("Book-Rating")) {
						try {
							avgRank += Integer.parseInt(temp[1].replaceAll(
									"\"", ""));
						} catch (NumberFormatException e) {
							System.err.println("## ERROR ## temp[1]: "
									+ temp[1]);
							e.printStackTrace();
						}
						nRanks++;
					}
				}

			}
			nRanks = nRanks <= 0 ? 1 : nRanks;
			avgRank = avgRank / nRanks;
			rank = String.valueOf((int) Math.round(avgRank));
			finalRes = year + "\t\"" + rank + "\"";
			context.write(key, new Text(finalRes));
		}
	}

	public static class AnalyzeMap extends
	Mapper<LongWritable, Text, Text, IntWritable> {
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String year = "";
			String rank = "";

			String[] arr = line.split("\t");

			if (Pattern.matches("\"[0-9]{4}\"", arr[1]))
				year = arr[1];
			if (Pattern.matches("\"[0-9]{1,2}\"", arr[2]))
				rank = arr[2];

			if (year.equals("\"2002\"")) {
				context.write(new Text(rank.replaceAll("\"", "")),
						new IntWritable(1));
			}
		}
	}

	public static class AnalyzeReduce extends
	Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> value,
				Context context) throws IOException, InterruptedException {
			int sum = 0;

			for (IntWritable x : value) {
				sum += x.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration joinConf = new Configuration();

		Job joinJob = Job.getInstance(joinConf,
				"Joining Books and Book-Ratings");
		joinJob.setJarByClass(BooksDataRanks.class);
		joinJob.setReducerClass(JoinReduce.class);
		joinJob.setOutputKeyClass(Text.class);
		joinJob.setOutputValueClass(Text.class);
		MultipleInputs.addInputPath(joinJob, new Path(args[0]),
				TextInputFormat.class, BookJoinMap.class);
		MultipleInputs.addInputPath(joinJob, new Path(args[1]),
				TextInputFormat.class, BooksDataRankJoinMap.class);
		joinJob.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(joinJob, new Path(args[2]));

		Path joinOutputPath = new Path(args[2]);
		joinOutputPath.getFileSystem(joinConf).delete(joinOutputPath, true);
		joinJob.waitForCompletion(true);

		// Second Map Reduce (Analyze)
		Configuration analyzeConf = new Configuration();

		Job analyzeJob = Job.getInstance(analyzeConf,
				"Number of books in 2002 on the basis of rank");
		analyzeJob.setJarByClass(BooksDataRanks.class);
		analyzeJob.setMapperClass(AnalyzeMap.class);
		analyzeJob.setReducerClass(AnalyzeReduce.class);
		analyzeJob.setOutputKeyClass(Text.class);
		analyzeJob.setOutputValueClass(IntWritable.class);
		analyzeJob.setInputFormatClass(TextInputFormat.class);
		analyzeJob.setOutputFormatClass(TextOutputFormat.class);
		
		
		FileInputFormat.setInputPaths(analyzeJob, new Path(args[2] + "/part*"));
		FileOutputFormat.setOutputPath(analyzeJob, new Path(args[3]));

		Path analyzeOutputPath = new Path(args[3]);
		analyzeOutputPath.getFileSystem(analyzeConf).delete(analyzeOutputPath,
				true);
		analyzeJob.waitForCompletion(true);
		joinOutputPath.getFileSystem(joinConf).delete(joinOutputPath, true);

		// Serially executing class
		JobControl jc = new JobControl("Join and Analyze Job Control");
		ControlledJob cJoinJob = new ControlledJob(joinConf);
		cJoinJob.setJob(joinJob);
		ControlledJob cAnalyzeJob = new ControlledJob(analyzeConf);
		cAnalyzeJob.setJob(analyzeJob);
		cAnalyzeJob.addDependingJob(cJoinJob);

		new Thread(jc).start();

		System.exit(jc.allFinished() ? 0 : 1);

	}

}