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

	private static final String TAG = "MovieStableActorReducer";
	
	LinkedHashMap<String, String> hMap = new LinkedHashMap<String, String>();
	int task = 0; 
	
	@Override
	public void reduce(DoublePair key, Iterable<TextPair> values,Context context) 
			throws IOException, InterruptedException {
		
		String popActor = "", actStd = "", meanVarStd = "";
		
		for (TextPair val:values) {
			popActor = val.getSecond().toString();	
			System.out.println(TAG + "popActor: " + popActor);
			actStd = val.getFirst().toString();
			System.out.println(TAG + "actStd: " + actStd);
			meanVarStd = key.getFirst().get() +"\t"+ key.getSecond().get()+"\t"+ actStd;
			System.out.println(TAG + "meanVarStd: " + meanVarStd);
			hMap.put(popActor,meanVarStd);  
		}
	}   	      

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {

		Iterator<String> i = hMap.keySet().iterator();
		while (i.hasNext()) {
			System.out.println(TAG + "cleanup meanVarStd: " + hMap.get(i.next()));
		}
		
		int count=0;
		context.write(new Text("Actor Name"), new Text("Popular Variance Stdev"));
		while (i.hasNext() && ++count < 6) {
			String finalKey = i.next();  
			context.write(new Text(finalKey), new Text(hMap.get(finalKey)));
		}
	}
}