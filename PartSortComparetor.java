package com.kjw.test;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PartSortComparetor extends WritableComparator{

	public PartSortComparetor() {
		super(PartSort.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable obj1, WritableComparable obj2) {
		PartSort data1 = (PartSort)obj1;
		PartSort data2 = (PartSort)obj2;
		
		return data1.compareTo(data2);
	}
}
