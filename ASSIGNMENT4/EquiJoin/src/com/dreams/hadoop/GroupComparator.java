package com.dreams.hadoop;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * GroupComparator
 * @author Selva
 *
 */
public class GroupComparator extends WritableComparator {
	
	protected GroupComparator() {
		super(CustomKey.class, true);
	}

	@Override
	public int compare(@SuppressWarnings("rawtypes") WritableComparable w1, @SuppressWarnings("rawtypes") WritableComparable w2) {
		CustomKey ck = (CustomKey) w1;
		CustomKey ck2 = (CustomKey) w2;
		return CustomKey.compare(ck.getStoreId(), ck2.getStoreId());
	}
}