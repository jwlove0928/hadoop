package com.kjw.test;

import org.apache.hadoop.io.Text;

public class AirlineP {

	private String UniqueCode; //항공사 코드
	private String TN; //항공기 등록번호
	private int Cancell; //비행 취소 여부
	private int CD; //항공사 지연여부
	
	final static int CANCELD = 0; //취소여부 0과 1 나뉨(1:예, 0:아니오)
	final static int NONDELAY = 0; //지연이 안된 값 0 
	
	private int getDigitFromStr(String str, int defaultDigit) {
		if("NA".equalsIgnoreCase(str)) return defaultDigit; //key값에 NA값이 있을경우 return시  NA값을 제외한 Delay값을 저장
		return Integer.parseInt(str);
	}
	
	public AirlineP () {
		
	}
	public AirlineP(Text value) {
		String[] AirData = value.toString().split(","); //데이터값을 "," 을 기준으로 값을 자름
		UniqueCode = AirData[8];
		TN = AirData[10];
		Cancell = Integer.parseInt(AirData[21]); //데이터값 21번째 있는 데이터를 가져와서 저장
		CD = getDigitFromStr(AirData[24], NONDELAY); 
	}

	public String getUniqueCode() {
		return UniqueCode;
	}

	public void setUniqueCode(String uniqueCode) {
		UniqueCode = uniqueCode;
	}

	public String getTN() {
		return TN;
	}

	public void setTN(String tN) {
		TN = tN;
	}

	public int getCancell() {
		return Cancell;
	}

	public void setCancell(int cancell) {
		Cancell = cancell;
	}

	public int getCD() {
		return CD;
	}
	
}
