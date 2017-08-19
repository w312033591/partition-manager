package com.hty.partition.exception;
/**
 * 自定义Partition异常
 * @author Hetianyi
 *
 */
public class PartitionException extends Exception {
	
	private static final long serialVersionUID = 7910343923047635698L;
	
	public PartitionException(String messgae) {
		super(messgae);
	}
	public PartitionException() {
		super();
	}
	
}
