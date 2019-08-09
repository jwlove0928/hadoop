package com.kjw.test;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class Partitioner01 extends Partitioner<PartSort, IntWritable>{

	@Override
	public int getPartition(PartSort key, IntWritable value, int numPartiions) {
		
		int hash = key.getUniqueCode().hashCode();
		int partition = hash % numPartiions;
		
		return partition;
	}
}
