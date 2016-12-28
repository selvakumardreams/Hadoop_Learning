package com.dreams.hadoop;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * StoreSalesMapper
 * @author selva
 *
 */
public class StoreSalesMapper extends Mapper<LongWritable, Text, CustomKey, Text> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, CustomKey, Text>.Context context)
			throws IOException, InterruptedException {

		String[] columns = value.toString().split(",");
		System.out.println("EquiJoin: " + columns[0] + ":" + columns[1] + ":" + columns[2]);
		if (columns != null) {
			context.write(new CustomKey(Integer.parseInt(columns[0]), 2), new Text(columns[1] + "\t" + columns[2]));
		}
	}
}