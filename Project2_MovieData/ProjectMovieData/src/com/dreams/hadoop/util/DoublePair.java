package com.dreams.hadoop.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Double Pair
 * @author Selva
 *
 */
public class DoublePair implements WritableComparable<DoublePair> {

	private DoubleWritable first;
	private DoubleWritable second;

	public DoublePair() {
		set(new DoubleWritable(), new DoubleWritable());
	}

	public DoublePair(Double first, Double second) {
		set(new DoubleWritable(first), new DoubleWritable(second));
	}

	public DoublePair(DoubleWritable first, DoubleWritable second) {
		set(first, second);
	}

	public void set(DoubleWritable first, DoubleWritable second) {
		this.first = first;
		this.second = second;
	}

	public DoubleWritable getFirst() {
		return first;
	}

	public DoubleWritable getSecond() { 
		return second;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}
	
	@Override
	public int hashCode() {
		return first.hashCode() * 163 + second.hashCode();
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof DoublePair) {
			DoublePair tp = (DoublePair) o;
			return first.equals(tp.first) && second.equals(tp.second);
		}
		return false;
	}
	
	
	@Override
	public int compareTo(DoublePair tp) {
		int cmp = first.compareTo(tp.first);
		if (cmp != 0) {
			return cmp;
		}
		return second.compareTo(tp.second);
	}
}