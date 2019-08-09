package com.kjw.test;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Reduce01 extends Reducer<PartSort, IntWritable, Text, Text>{

	@Override
	protected void reduce(PartSort key, Iterable<IntWritable> values, Context ctx) throws IOException, InterruptedException {
		int TotalCnt = 0;
		int DealyCnt = 0;
		for (IntWritable v : values) { //받아온 값을 count
			TotalCnt++;
			if(v.get()>AirlineP.NONDELAY) { //0보다 큰 경우는 delay가 되었기 때문에 delay값에 ++
				DealyCnt++;
			}
		}
		ctx.write(new Text(key.getUniqueCode()+"_"+key.getTN()),new Text((float)DealyCnt/(float)TotalCnt+""));
		//text형식으로 key값을 항공사 코드와 등록번호로 보내고, Map에서 받아온 값으로 낙후도를 구하고 그 값을 전달
	}
}
