package com.hty.partition.exception;

public class IllegalSqlException extends IllegalStateException {
	
	private static final long serialVersionUID = 119306034096067426L;
	
	public IllegalSqlException(String messgae) {
		super(messgae);
	}
	public IllegalSqlException() {
		super();
	}
}
