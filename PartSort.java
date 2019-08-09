package com.kjw.test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class PartSort extends AirlineP implements WritableComparable<PartSort>{

	public PartSort() { //default
		
	}
	
	public PartSort(Text value) {
		super(value);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.setUniqueCode(WritableUtils.readString(in));
		
		super.setTN(WritableUtils.readString(in));
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, super.getUniqueCode());
		WritableUtils.writeString(out, super.getTN());
		
	}

	@Override
	public int compareTo(PartSort data) {
		int result = super.getUniqueCode().compareTo(data.getUniqueCode());
		
		if(0==result) {
			return super.getTN().compareTo(data.getTN())*-1;
		}
		return result;
	}
	
}
