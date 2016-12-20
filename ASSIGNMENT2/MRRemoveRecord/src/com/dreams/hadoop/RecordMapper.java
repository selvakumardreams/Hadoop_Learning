package com.dreams.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RecordMapper extends
Mapper<Object, Text, Text, NullWritable> {
	
	private static final int RECORD_LENGTH = 15;
	@Override
	public void map(Object key, Text row, Context con) {
		try {
			System.out.println("MAPPER : "+ row.getLength());
			if (row.getLength() >= RECORD_LENGTH) {
				con.write(row, NullWritable.get());
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}