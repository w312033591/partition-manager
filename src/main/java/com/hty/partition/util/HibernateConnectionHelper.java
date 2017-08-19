package com.hty.partition.util;

import java.sql.Connection;
import java.sql.SQLException;

import org.hibernate.jdbc.Work;
/**
 * DAO获取Hibernate的数据库连接辅助类
 * @author Tisnyi
 */
public class HibernateConnectionHelper implements Work {
	/** 数据库连接 */
	private Connection conn;
	
	public void execute(Connection conn) throws SQLException {
		this.conn = conn;
	}
	
	public Connection getConnection() {
		return this.conn;
	}
}
