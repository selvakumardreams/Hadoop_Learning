package com.dreams.hadoop.util;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * StableGroupComp
 * @author Selva
 *
 */
public class StableGroupComp extends WritableComparator {

	protected StableGroupComp() {
		super(DoublePair.class, true);
	}   
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		DoublePair k1 = (DoublePair)w1;
		DoublePair k2 = (DoublePair)w2;

		Double val1 = k1.getFirst().get();
		Double val2 = k2.getFirst().get();
		Double val3 = k1.getSecond().get();
		Double val4 = k2.getSecond().get();
		// Perform grouping only based on Mean
		int result = val1.compareTo(val2);
		if(result == 0) {
			result=val3.compareTo(val4);
		}
		return result;
	}	        
}