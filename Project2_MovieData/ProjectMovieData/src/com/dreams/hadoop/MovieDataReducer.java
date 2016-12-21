package com.dreams.hadoop;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.dreams.hadoop.util.TextPair;
import com.google.common.collect.Ordering;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;

class MovieDataReducer extends Reducer<IntWritable,TextPair,Text,Text> {

	String delim2 =";";
	String header1 = "MOVIE_YEAR", header3 = "POPULARITY",header2 = "";

	SortedSetMultimap<Integer,String> sortmulMap=TreeMultimap.create(Ordering.natural().reverse(), Ordering.natural());

	public void reduce(IntWritable key, Iterable<TextPair> values,Context context) 
			throws IOException, InterruptedException {

		String[] value1 = new String[100]; 
		String value2 = "";
		int value1Pop=0;

		for (TextPair val:values) {
			value1 = val.getFirst().toString().split(delim2);
			value1Pop = Integer.parseInt(value1[1]);


			value2 = val.getSecond().toString();

			sortmulMap.put(value1Pop,value1[0]+"\t"+value1[2]);
		}   
		
		header2 = value2 + "\t";

	}  

	public void cleanup(Context context) throws IOException, InterruptedException {
		Set<Integer> keys = sortmulMap.keySet();

		int loop = 0;	
		for (Integer key : keys) {
			loop += 1;
			Iterator<String>  c = sortmulMap.get(key).iterator();
			while(c.hasNext()) {
				String op_value = c.next().toString();																																																																																																																																																														
				context.write(new Text(key.toString()),new Text(op_value));
			}

			if(loop==1) {
				break;
			}
		}																																																																																																																																																																																																																																																																																									
	}
}