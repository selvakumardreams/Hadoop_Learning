package com.dreams.hadoop;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StoreMapper extends Mapper<LongWritable, Text, CustomKey, Text> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, CustomKey, Text>.Context context)
			throws IOException, InterruptedException {
		String[] columns = value.toString().split(",");
		int storeId = Integer.parseInt(columns[0]);
		System.out.println("EquiJoin: " + storeId);
		context.write(new CustomKey(storeId, 1), new Text(columns[1] + "\t" + columns[2]));
	}
}