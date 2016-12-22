package com.dreams.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.dreams.hadoop.util.DoublePair;
import com.dreams.hadoop.util.TextPair;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

/**
 * MovieStableActorMapper
 * @author selva
 *
 */
public class MovieStableActorMapper extends Mapper<LongWritable, Text, DoublePair, TextPair> {
	
	private static final String TAG = "MovieStableActorMapper";

	Multimap<String, Integer> multiMap = ArrayListMultimap.create();


	@Override  
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String delim = ";";
		String[] line = value.toString().split(delim);

		if(!line[4].equals("Actor") && 
				!line[7].equals("Popularity") && 
				!line[4].equals("CAT") && 
				!line[7].equals("INT") &&
				!line[4].equals("") && 
				!line[7].equals("")) {

			int popularity = Integer.parseInt(line[7]);
			String actor = line[4].trim();
			multiMap.put(actor, popularity);
		}
	}	 
	
	/**
	 * cleanup
	 */
	public void cleanup(Context context) throws IOException, InterruptedException {

		Set<String> keys = multiMap.keySet(); 

		for (String key : keys) {

			ArrayList<Integer> pop= new ArrayList<Integer>();

			Iterator<Integer>  c = multiMap.get(key).iterator();

			while(c.hasNext()) {
				pop.add(c.next());
			}

			Double popMean = calculateMean(pop);
			System.out.println(TAG + "popMean: " + popMean);
			Double popVariance = calculateVariance(pop); 
			System.out.println(TAG + "popVariance: " + popVariance);
			Double popSd =  Math.sqrt(popVariance);
			System.out.println(TAG + "popSd: " + popSd);
			pop.clear();

			String popSdString = popSd.toString();
			context.write(new DoublePair(new DoubleWritable(popMean), 
					new DoubleWritable(popVariance)), 
					new TextPair(new Text(popSdString), 
							new Text(key)));
		}
	}

	/**
	 * computeMean
	 * @param popularity
	 * @return
	 */
	public double calculateMean(ArrayList<Integer> popularity) {
		int popTotal = 0;	
		
		for(int i=0;i<popularity.size();i++) {
			popTotal += popularity.get(i);		
			System.out.println(TAG + "Total: " + popTotal + "Popularity :" + popularity.get(i));
		}
		return (popTotal/popularity.size());		
	}

	/**
	 * computeVariance
	 * @param popularity
	 * @return
	 */
	public double calculateVariance(ArrayList<Integer> popularity) {
		if (popularity.size()==0)   return Double.NaN;
		double avg = calculateMean(popularity);
		double sum = 0.0;
		for(int i=0;i<popularity.size();i++) {
			sum+= (popularity.get(i)-avg) *  (popularity.get(i)-avg);
		}
		return (sum/popularity.size());					
	}
} 