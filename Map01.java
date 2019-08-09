package com.kjw.test;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Map01 extends Mapper<LongWritable, Text, PartSort, IntWritable>{

	@Override
	public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
		PartSort data = new PartSort(value);
		
		if(data.getCancell() == AirlineP.CANCELD) {
			ctx.write(data, new IntWritable(data.getCancell())); //data값이 0일때 즉 취소가 안된것이기때문에 그 값만 key,value로 전송
		}
	}
}
