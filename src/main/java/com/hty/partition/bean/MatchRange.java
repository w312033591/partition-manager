package com.hty.partition.bean;
/**
 * 函数匹配范围用于替换
 * @author Tisnyi
 */
public class MatchRange {
	private int start;
	private int end;
	private String trueTable;
	
	public MatchRange(int start, int end, String trueTable) {
		this.start = start;
		this.end = end;
		this.trueTable = trueTable;
	}
	
	public int getStart() {
		return start;
	}
	
	public void setStart(int start) {
		this.start = start;
	}
	
	public int getEnd() {
		return end;
	}
	
	public void setEnd(int end) {
		this.end = end;
	}

	public String getTrueTable() {
		return trueTable;
	}

	public void setTrueTable(String trueTable) {
		this.trueTable = trueTable;
	}
}
