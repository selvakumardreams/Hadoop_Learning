package com.dreams.hadoop;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.dreams.hadoop.util.DoublePair;
import com.dreams.hadoop.util.TextPair;

/**
 * MovieStableDirectorReducer
 * @author selva
 *
 */

class MovieStableDirectorReducer extends Reducer <DoublePair,TextPair,Text,Text> {

	LinkedHashMap<String, String> hMap = new LinkedHashMap<String, String>();
	int task = 0; 

	public void reduce(DoublePair key, Iterable<TextPair> values,Context context) 
			throws IOException, InterruptedException {
		String popDirector = "", directorStd = "", meanVarStd = "";

		for (TextPair val:values) {
			popDirector = val.getSecond().toString();		   
			directorStd = val.getFirst().toString();
			meanVarStd = key.getFirst().get() +"\t"+ key.getSecond().get()+"\t"+ directorStd;

			hMap.put(popDirector,meanVarStd);  	   

		}

	}   	      


	public void cleanup(Context context) throws IOException, InterruptedException {

		Iterator<String> i=hMap.keySet().iterator();
		int count = 0;
		context.write(new Text("Director Name"), new Text("Popularity Variance Stdev"));
		while(i.hasNext() && ++count < 6) {
			String finalKey = i.next();  
			context.write(new Text(finalKey), new Text(hMap.get(finalKey)));
		}
	}
}
