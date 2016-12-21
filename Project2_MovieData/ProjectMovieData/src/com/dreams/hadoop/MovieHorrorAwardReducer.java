package com.dreams.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * MovieHorrorAwardReducer
 * @author Selva
 *
 */

public class MovieHorrorAwardReducer extends Reducer<Text,IntWritable,Text,IntWritable> 
{
	String delim =";";
	String header1 = "HORROR_MOVIE", header2 = "COUNT";

	public void setup(Context context) throws IOException, InterruptedException {
		context.write(new Text(header1+"\t\t"+header2), new IntWritable(1));
	}

	public void reduce(Text key, Iterable<IntWritable> values,Context context) 
			throws IOException, InterruptedException { 
		int sum = 0;
		for (IntWritable val:values) {
			sum+=val.get();
		}   
		context.write(new Text(key), new IntWritable(sum));
	}  
}