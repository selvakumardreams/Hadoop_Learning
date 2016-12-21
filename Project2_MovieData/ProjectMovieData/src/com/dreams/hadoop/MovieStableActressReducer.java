package com.dreams.hadoop;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.dreams.hadoop.util.DoublePair;
import com.dreams.hadoop.util.TextPair;

/**
 * MovieStableActressReducer
 * @author Selva
 *
 */
public class MovieStableActressReducer extends Reducer <DoublePair,TextPair,Text,Text> {

	LinkedHashMap<String, String> hMap = new LinkedHashMap<String, String>();
	int task = 0; 

	public void reduce(DoublePair key, Iterable<TextPair> values,Context context) 
			throws IOException, InterruptedException {
		
		String popActress = "", actressStd = "", meanVarStd = "";

		for (TextPair val:values) {
			popActress = val.getSecond().toString();		   
			actressStd = val.getFirst().toString();
			meanVarStd = key.getFirst().get() +"\t"+ key.getSecond().get()+"\t"+ actressStd;
			hMap.put(popActress,meanVarStd);  	   
		}

	}   	      


	public void cleanup(Context context) throws IOException, InterruptedException {

		Iterator<String> i=hMap.keySet().iterator();
		int count = 0;
		context.write(new Text("Actress Name"), new Text("Popularity Variance Stdev"));
		while(i.hasNext() && ++count < 6) {
			String finalKey = i.next();  
			context.write(new Text(finalKey), new Text(hMap.get(finalKey)));
		}
	}
}