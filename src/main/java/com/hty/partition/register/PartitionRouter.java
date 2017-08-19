package com.hty.partition.register;

/**
 * 根据ID定义自解析分表信息的路由类。<br>
 * 这里的业务对象ID必须是字符串类型
 * @author Hetianyi
 *
 */
public abstract class PartitionRouter {
	/**
	 * 根据业务对象ID定位分表名称（如根据订单ID确定 分表）
	 * @param id
	 * @return
	 */
	public abstract int getPartition(String businessId) throws IllegalArgumentException;
	/**
	 * 根据业务数据的服务对象ID对分表数量取模确定分表名称，服务对象ID可以是用户ID，论坛板块ID等。
	 * @param serviceObject
	 * @return
	 * @throws IllegalArgumentException
	 */
	public abstract int getPartition(long serviceObjectId, int partitionCount) throws IllegalArgumentException;
	
}
