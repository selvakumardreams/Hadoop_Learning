package com.dreams.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RecordReducer extends
Reducer<Text, NullWritable, Text, NullWritable> {
	private static final int RECORD_LENGTH = 15;
	@Override
	public void reduce(Text key, Iterable<NullWritable> Value, Context con) {
		try {
			System.out.println("REDUCER : "+ key.getLength());
			if (key.getLength() >= RECORD_LENGTH) {
				con.write(key, NullWritable.get());
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}