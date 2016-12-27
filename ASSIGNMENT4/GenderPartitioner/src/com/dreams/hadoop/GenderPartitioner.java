package com.dreams.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * GenderPartitioner
 * @author Selva
 *
 */
public class GenderPartitioner {
	
	private static final String TAG = "GenderPartitioner";
	
	public static class GenderPartitionerMapper extends
	Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			try {
				String[] tokens = value.toString().split("\\t|\\s+");

				String gender = tokens[2].toString();
				System.out.println(TAG + ": "+ gender);
				
				String nameAge = tokens[0] + "\t" + tokens[1];
				System.out.println(TAG + ": "+ nameAge);
				context.write(new Text(gender), new Text(nameAge));
			} catch (ArrayIndexOutOfBoundsException e) {
				e.printStackTrace();
			}

		}
	}

	public static class AgePartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {

			String [] nameAgeScore = value.toString().split("\\t|\\s+");
			String age = nameAgeScore[1];
			int ageInt = Integer.parseInt(age);
			
			System.out.println(TAG + "AgePartitioner: "+ ageInt);
			
			if(numReduceTasks == 0)
				return 0;

			if(ageInt <=20) {				
				return 0;
			}
			if(ageInt >20 && ageInt <=50){

				return 1 % numReduceTasks;
			} else
				return 2 % numReduceTasks;

		}
	}


	public static class GenderPartitionerReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {


			String name = " ";
			String age = " ";
			String gender = " ";
			
			for(Text val: values) {

				String [] valTokens = val.toString().split("\\t");

				System.out.println(TAG + "Reducer: "+ valTokens[0] + ": " + valTokens[1]);

				name = valTokens[0];
				age = valTokens[1];
				gender = key.toString();
				context.write(new Text(name), new Text("Age- "+ age + "\t" + gender ));
			}
			
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		try {
			Job job = Job.getInstance(conf, "GenderPartitioner");
			job.setJarByClass(GenderPartitioner.class);
			job.setMapperClass(GenderPartitionerMapper.class);
			job.setReducerClass(GenderPartitionerReducer.class);
			job.setPartitionerClass(AgePartitioner.class);
			job.setNumReduceTasks(3);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			System.exit(job.waitForCompletion(true) ? 1 : 0);
		} catch (IOException e) {

		}
		
	}

}