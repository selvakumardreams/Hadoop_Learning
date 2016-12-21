package com.dreams.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.dreams.hadoop.util.IntPair;

/**
 * 
 * @author Selva
 *
 */
public class ActorActressPairMapper extends Mapper<LongWritable, Text, Text, IntPair> {

	@Override  
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {

		String delim = ";";
		String[] line = value.toString().split(delim);

		if(!line[4].equals("Actor") &&
				!line[5].equals("Actress") &&
				!line[7].equals("Popularity") && 
				!line[5].equals("CAT") && 
				!line[7].equals("CAT") && 
				!line[7].equals("CAT") && 
				!line[4].equals("") && 
				!line[5].equals("") &&
				!line[7].equals("") &&
				!line[8].equals("Awards") && 
				!line[8].equals("BOOL") && 
				!line[8].equals("")) {

			int popularity = Integer.parseInt(line[7]);
			String awards = line[8].trim();
			int awardInd = Integer.parseInt((awards.equals("Yes")) ? "0" : "1");

			String actorActress = line[4].trim() + ";" +line[5].trim();

			context.write(new Text(actorActress), new IntPair(new IntWritable(popularity),new IntWritable(awardInd)));

		}

	}


} 