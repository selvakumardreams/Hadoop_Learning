package com.dreams.hadoop;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.dreams.hadoop.util.IntPair;

/**
 * 
 * @author Selva
 *
 */
public class ActorActressPairReducer extends Reducer<Text,IntPair,Text,LongWritable> {

	TreeMap<Long,String> countMap = new TreeMap<Long,String>();

	public void reduce(Text key, Iterable<IntPair> values,Context context) 
			throws IOException, InterruptedException {

		int count = 0;
		long sumMoviePopularity = 0, avgMoviePopularity = 0, moviePopularity = 0;
		int moviePop = 0,movieAwards = 0; 

		for (IntPair val:values) {
			moviePop = val.getFirst().get();
			movieAwards = val.getSecond().get();
			if(movieAwards == 1) {
				moviePopularity = Math.round(moviePop + moviePop*0.1);
			} else {
				moviePopularity = moviePop;
			}

			sumMoviePopularity += moviePopularity;
			count += 1;

		}
		avgMoviePopularity = Math.round(sumMoviePopularity/count);
		countMap.put(avgMoviePopularity,key.toString());
	}   



	public void cleanup(Context context) throws IOException, InterruptedException {

		NavigableMap<Long, String> nMap = countMap.descendingMap();

		int count = 0;
		context.write(new Text("ACTOR-ACTRESS PAIR\t\tPOPULARITY"), new LongWritable(0));

		for(Map.Entry<Long, String> entry : nMap.entrySet()) { 
			count ++;
			if (count < 4) {
				long popul = entry.getKey();
				String actorActress = entry.getValue();
				context.write(new Text(actorActress),new LongWritable(popul));
			}
			else break;

		}																										
	}
}