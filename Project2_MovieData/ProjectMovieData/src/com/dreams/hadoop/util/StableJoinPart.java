package com.dreams.hadoop.util;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * StableJoinPart
 * @author Selva
 *
 */
public class StableJoinPart extends Partitioner<DoublePair, TextPair> {
	  
    @Override
    public int getPartition(DoublePair key, TextPair val, int numPartitions) {
    	Double value = key.getFirst().get();
       
    	int hash = value.hashCode() & Integer.MAX_VALUE;
    	       		   	
        int partition = hash % numPartitions;
        return partition;
    }
}
 
