package com.dreams.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


class MovieHorrorAwardMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override  
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException { 

		String delim = ";";
		String[] line= value.toString().split(delim);
		
		if( !line[8].equals("Awards") && 
				!line[0].equals("Year") &&
				!line[8].equals("BOOL") && 
				!line[0].equals("INT") &&
				!line[8].equals("") && 
				!line[0].equals("")) {

			String awards = line[8].trim();
			int year = Integer.parseInt(line[0]);

			if (year >= 1952 && year <= 1968) {
				context.write(new Text(awards), new IntWritable(1));
			}

		}

	}

} 