package com.dreams.hadoop.util;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * StableSortComp
 * @author Selva
 *
 */
public class StableSortComp extends WritableComparator {

	protected StableSortComp() {
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

		int result = (-1) * val1.compareTo(val2);        		

		if(0 == result) {
			result = val3.compareTo(val4);            		
		} 
		return result; 
	}	
}