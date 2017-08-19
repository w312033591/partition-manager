package com.hty.partition.dao;

import java.lang.reflect.Proxy;

import org.hibernate.SessionFactory;
/**
 * DAO代理工厂
 * @author Hetianyi
 */
public class PartitionDaoFactory {
	
	public static BaseDao createProxyDao(SessionFactory sessionFactory) 
			throws Exception {
		BaseDao dao = (BaseDao) Proxy.newProxyInstance(BaseDao.class.getClassLoader(), 
				new Class<?>[]{BaseDao.class}, new BaseDaoInvocationHandler(1, sessionFactory));
		return dao;
	}
}
