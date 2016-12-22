package com.dreams.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.dreams.hadoop.util.TextPair;

class MovieDataMapper extends Mapper<LongWritable, Text, IntWritable, TextPair> {

	@Override  
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String delim = ";";
		String[] line = value.toString().split(delim);

		if((!line[2].equals("Title") && 
				!line[7].equals("Popularity") && 
				!line[0].equals("Year")) &&
				(!line[2].equals("STRING") && 
						!line[7].equals("CAT") && 
						!line[0].equals("INT")) &&
						(!line[2].equals("") && 
								!line[7].equals("") && 
								!line[0].equals(""))) {

			String moviePopularityYear = line[2] + ";" + line[7] + ";" + line[0];

			int year = Integer.parseInt(line[0]);

			if (year >= 1920 && year <= 1929) {
				System.out.println("1920-1929: " + moviePopularityYear);
				context.write(new IntWritable(1),new TextPair(new Text(moviePopularityYear),
						new Text("1920-1929"))); 
			} else if (year >= 1930 && year <= 1939) { 
				System.out.println("1930-1939: " + moviePopularityYear);
				context.write(new IntWritable(2),new TextPair(new Text(moviePopularityYear),
						new Text("1930-1939")));
			} else if (year >= 1940 && year <= 1949) {
				System.out.println("1940-1949: " + moviePopularityYear);
				context.write(new IntWritable(3),new TextPair(new Text(moviePopularityYear),
						new Text("1940-1949")));
			} else if (year >= 1950 && year <= 1959) {
				System.out.println("1950-1959: " + moviePopularityYear);
				context.write(new IntWritable(4),new TextPair(new Text(moviePopularityYear),
						new Text("1950-1959")));
			} else if (year >= 1960 && year <= 1969) {
				System.out.println("1960-1969: " + moviePopularityYear);
				context.write(new IntWritable(5),new TextPair(new Text(moviePopularityYear),
						new Text("1960-1969")));
			} else if (year >= 1970 && year <= 1979) {
				System.out.println("1970-1979: " + moviePopularityYear);
				context.write(new IntWritable(6),new TextPair(new Text(moviePopularityYear),
						new Text("1970-1979")));
			} else if (year >= 1980 && year <= 1989) {
				System.out.println("1980-1989: " + moviePopularityYear);
				context.write(new IntWritable(7),new TextPair(new Text(moviePopularityYear),
						new Text("1980-1989")));
			} else if (year >= 1990 && year <= 1999) {
				System.out.println("1990-1999: " + moviePopularityYear);
				context.write(new IntWritable(8),new TextPair(new Text(moviePopularityYear),
						new Text("1990-1999")));
			} else { 
				System.out.println("2000-greater: " + moviePopularityYear);
				context.write(new IntWritable(9),new TextPair(new Text(moviePopularityYear),
						new Text("2000 or greater")));
			}

		}

	}

} 