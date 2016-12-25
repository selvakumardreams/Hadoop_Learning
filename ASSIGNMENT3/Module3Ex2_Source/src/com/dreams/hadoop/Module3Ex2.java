package com.dreams.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Module3Ex2
 * @author Selva
 *
 */
public class Module3Ex2 {
	
	private static final String TAG = "Module3Ex2";
	public static void main(String args[]) {
		try {
			Configuration conf = new Configuration();
			String[] otherArgs = new GenericOptionsParser(conf, args)
			.getRemainingArgs();
			if (otherArgs.length != 2) {
				System.err.println("Usage: Module3Ex2 <in> <out>");
				System.exit(2);
			}
			Job job = Job.getInstance(conf, "Module3Ex2");
			job.setJarByClass(Module3Ex2.class);
			job.setMapperClass(Module3Ex2Mapper.class);
			job.setCombinerClass(Module3Ex2Reducer.class);
			job.setReducerClass(Module3Ex2Reducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(MinMaxCountTuple.class);
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}


	public static class Module3Ex2Mapper extends
	Mapper<Object, Text, Text, MinMaxCountTuple> {

		private Text dataSet = new Text();
		private MinMaxCountTuple outPut = new MinMaxCountTuple();
		private Double minValue;
		private Double maxValue;

		@Override
		protected void map(Object key, Text value,
				Context context)
						throws IOException, InterruptedException {
			
			String[] fields = value.toString().split("\\t|\\s+");
			
			try {
				dataSet.set(fields[0]);
				minValue = Double.parseDouble(fields[2]);
				System.out.println(TAG + "minValue : " + minValue);
				maxValue = Double.parseDouble(fields[2]);
				System.out.println(TAG + "maxValue : " + maxValue);
			} catch (ArrayIndexOutOfBoundsException e) {
				e.printStackTrace();
			}

			if (dataSet == null) {
				return; //Nothing to do
			}

			try {
				outPut.setMin(minValue);
				outPut.setMax(maxValue);
				outPut.setCount(1);
				context.write(dataSet, outPut);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}


	public static class Module3Ex2Reducer extends
	Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple> {
		private MinMaxCountTuple resultRow = new MinMaxCountTuple();

		@Override
		protected void reduce(Text key, Iterable<MinMaxCountTuple> values,
				Context context)
						throws IOException, InterruptedException {
			
			resultRow.setMin(null);
			resultRow.setMax(null);
			resultRow.setCount(0);
			int sum = 0;


			for (MinMaxCountTuple val : values) {
				if (resultRow.getMin() == null
						|| val.getMin().compareTo(resultRow.getMin()) < 0 ) {
					System.out.println(TAG + "getMin : " + val.getMin());
					resultRow.setMin(val.getMin());
				}
				
				if (resultRow.getMax() == null
						|| val.getMax().compareTo(resultRow.getMax()) > 0 ) {
					System.out.println(TAG + "getMax : " + val.getMax());
					resultRow.setMax(val.getMax());
				}
				
				
				sum += val.getCount();
				System.out.println(TAG + "sum : " + sum);
			}
			
			resultRow.setCount(sum);
			context.write(key, resultRow);

		}


	}

	public static class MinMaxCountTuple implements Writable {

		private long count = 0;
		private Double  min;
		private Double max;


		public long getCount() {
			return count;
		}

		public void setCount(long count) {
			this.count = count;
		}

		public Double getMin() {
			return min;
		}

		public void setMin(Double min) {
			this.min = min;
		}

		public Double getMax() {
			return max;
		}

		public void setMax(Double max) {
			this.max = max;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			min = in.readDouble();
			max = in.readDouble();
			count = in.readLong();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeDouble(min);
			out.writeDouble(max);
			out.writeLong(count);
		}

		@Override
		public String toString() {
			return min + "\t" + max + "\t" + count;
		}

	}
}
