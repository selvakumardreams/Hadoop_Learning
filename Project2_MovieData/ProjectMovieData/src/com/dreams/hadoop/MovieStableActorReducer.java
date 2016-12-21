package com.dreams.hadoop;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.dreams.hadoop.util.DoublePair;
import com.dreams.hadoop.util.TextPair;

/**
 * MovieStableActorReducer
 * @author selva
 *
 */
public class MovieStableActorReducer extends Reducer <DoublePair,TextPair,Text,Text> {

	LinkedHashMap<String, String> hMap = new LinkedHashMap<String, String>();
	int task = 0; 

	public void reduce(DoublePair key, Iterable<TextPair> values,Context context) 
			throws IOException, InterruptedException {
		
		String popActor = "", actStd = "", meanVarStd = "";
		
		for (TextPair val:values) {
			popActor = val.getSecond().toString();		   
			actStd = val.getFirst().toString();
			meanVarStd = key.getFirst().get() +"\t"+ key.getSecond().get()+"\t"+ actStd;
			hMap.put(popActor,meanVarStd);  	   
		}
	}   	      


	public void cleanup(Context context) throws IOException, InterruptedException {

		Iterator<String> i=hMap.keySet().iterator();
		int count=0;
		context.write(new Text("Actor Name"), new Text("Popularity Variance Stdev"));
		while (i.hasNext() && ++count < 6) {
			String finalKey = i.next();  
			context.write(new Text(finalKey), new Text(hMap.get(finalKey)));
		}
	}
}