package com.dreams.hadoop;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * StoreSalesReducer
 * @author selva
 *
 */
public class StoreSalesReducer extends Reducer<CustomKey, Text, IntWritable, Text> {
	@Override
	protected void reduce(CustomKey key, Iterable<Text> values,
			Reducer<CustomKey, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
		Iterator<Text> itr = values.iterator();
		Text storeName = new Text(itr.next());
		while (itr.hasNext()) {
			Text salesInfo = itr.next();
			Text storeSalesInfo = new Text(storeName.toString() + "\t" + salesInfo.toString());
			System.out.println("EquiJoin: " + storeName.toString() + ":" + salesInfo.toString());
			context.write(new IntWritable(key.getStoreId()), storeSalesInfo);
		}
	}
}