package com.hty.partition.register.dialectimpl;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.persistence.Column;
import javax.sql.DataSource;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hty.partition.register.PartitionEntity;
import com.hty.partition.register.PartitionRouter;
import com.hty.partition.util.MethodUtil;
/**
 * MySQL数据库分表实现类
 * @author Hetianyi
 */
public class MysqlPartitionEntity<T> extends PartitionEntity<T> {
	private Log logger;
	private boolean init = false;
	
	public MysqlPartitionEntity(Class<T> clazz, int partitionCount, PartitionRouter router, 
			DataSource dataSource, boolean checkPartitionTables, String create_table_ddl) 
			throws Exception {
		super(clazz, partitionCount, router, dataSource, checkPartitionTables, create_table_ddl);
	}
	/**
	 * 初始化数据库驱动类
	 * @throws ClassNotFoundException
	 */
	private void init() throws ClassNotFoundException {
		if(!init) {
			logger = LogFactory.getLog(MysqlPartitionEntity.class);
			Class.forName("com.mysql.jdbc.Driver");
			init = true;
		}
	}
	/** 获取一次性数据库连接 */
	private Connection getDBConnection() throws SQLException {
		return this.dataSource.getConnection();
	}
	
	/** 关闭一次性数据库连接 */
	private void closeConnection(Connection conn) throws SQLException {
		conn.close();
	}
	
	@Override
	public void checkPartitionTablesAndView() throws Exception {
		int createTable = 0;
		boolean createView = false;
		//初始化
		init();
		logger.info("Checking partition tables for entity:" + this.entityClass.getName() + ".");
		//获取数据库连接
		Connection conn = getDBConnection();
		//生成视图创建语句
		StringBuilder sb = new StringBuilder("CREATE VIEW ").append(this.viewName).append(" AS \n");
		//记录已存在的分表
		Set<String> existsPartitionTables = new HashSet<String>();
		
		//从_00开始查询分表table是否存在
		String sql = "select a.TABLE_NAME from information_schema.`TABLES` a where a.TABLE_NAME like ? and a.TABLE_TYPE='BASE TABLE'";
		PreparedStatement ps = conn.prepareStatement(sql);
		ps.setString(1, this.getViewName() + "_%");
		ResultSet rs = ps.executeQuery();
		rs.beforeFirst();
		while(rs.next()) {
			logger.info("Partiton table exists:" + rs.getString(1).toLowerCase());
			existsPartitionTables.add(rs.getString(1).toLowerCase());
		}
		rs.close();
		ps.close();
		//创建缺失表
		Statement state = conn.createStatement();
		int fixLen = 0;
		for(Iterator<String> it = this.partitionIdMapTable.values().iterator(); it.hasNext();) {
			String tn = it.next();
			if(fixLen == 0) {
				sb.append("SELECT * FROM ").append(tn);
			} else {
				sb.append("\nUNION\nSELECT * FROM ").append(tn);
			}
			if(!existsPartitionTables.contains(tn)) {
				logger.info("Create partition table:\n" + create_table_ddl.replace("$TABLE", tn));
				state.executeUpdate(create_table_ddl.replace("$TABLE", tn));
				createTable++;
			}
			fixLen++;
		}
		state.close();
		sql = "select a.TABLE_NAME from information_schema.`TABLES` a where a.TABLE_NAME = ? and a.TABLE_TYPE='VIEW'";
		ps = conn.prepareStatement(sql);
		ps.setString(1, this.getViewName());
		rs = ps.executeQuery();
		rs.beforeFirst();
		boolean existView = false;
		while(rs.next()) {
			logger.info("Partiton view exist:" + rs.getString(1).toLowerCase());
			existView = true;
		}
		rs.close();
		ps.close();
		if(!existView) {
			logger.info("Create entity view:\n" + sb.toString());
			state = conn.createStatement();
			state.executeUpdate(sb.toString());
			createView = true;
		}
		state.close();
		closeConnection(conn);
		logger.info("Partition tables check finished, create partition tables["+ createTable +"], create partition view["+ String.valueOf(createView) +"].");
	}
	
	/** 按照占位符产生的字段Getter方法列表 */
	private List<Method> insertFieldList;
	/** 新增SQL模板 */
	private StringBuilder insertSQL;
	@Override
	public String createInsertSQL(Object entity) throws Exception {
		if(null != insertSQL) {
			return insertSQL.toString();
		}
		String id = BeanUtils.getSimpleProperty(entity, this.idField.getName());
		if(null == id || "".equals(id.trim())) {
			throw new IllegalArgumentException("Entitiy id cannot be null!");
		}
		String table = this.translatePartitionTable(id);
		if(null == table) {
			throw new IllegalArgumentException("Cannot determine which table to insert into according entitiy id : " + id + ".");
		}
		insertSQL  = new StringBuilder("insert into ").append(table).append(" \n(\n");
		Field[] fs = this.getEntityClass().getDeclaredFields();
		insertFieldList = new ArrayList<Method>(fs.length);
		int appendTimes = 0;
		StringBuilder placeHolders = new StringBuilder("values(");
		for(int i = 0; i < fs.length; i++) {
			//没有get，set方法忽略
			if(!MethodUtil.hasGetterAndSetter(this.getEntityClass(), fs[i])) {
				continue;
			}
			try {
				insertFieldList.add(MethodUtil.getGetterMethod(entityClass, 
						MethodUtil.getGetterMethodName(fs[i].getName(), 
								(fs[i].getType() == Boolean.class || fs[i].getType() == boolean.class))));
			} catch (Exception e) {
				insertSQL = null;
				insertFieldList = null;
				throw new IllegalStateException("No getter method for field " + entityClass.getName() + "." + fs[i].getName(), e);
			}
			if(appendTimes > 0) {
				insertSQL.append(",\n");
				placeHolders.append(", ");
			}
			Column col = fs[i].getAnnotation(Column.class);
			insertSQL.append("\t").append(null == col ? fs[i].getName() : col.name());
			appendTimes++;
			placeHolders.append("?");
		}
		placeHolders.append(")");
		insertSQL.append("\n)\n");
		insertSQL.append(placeHolders.toString());
		return insertSQL.toString();
	}
	
	
	/** 按照占位符产生的字段Getter方法列表 */
	private List<Method> updateFieldList;
	/** 新增SQL模板 */
	private StringBuilder updateSQL;
	@Override
	public String createUpdateSQL(Object entity) throws Exception {
		if(null != updateSQL) {
			return updateSQL.toString();
		}
		String id = BeanUtils.getSimpleProperty(entity, this.idField.getName());
		if(null == id || "".equals(id.trim())) {
			throw new IllegalArgumentException("Entitiy Id cannot be null!");
		}
		String table = this.translatePartitionTable(id);
		if(null == table) {
			throw new IllegalArgumentException("Cannot determine which table to insert into according entitiy id : " + id + ".");
		}
		updateSQL  = new StringBuilder("update ").append(table).append(" a set\n");
		Field[] fs = this.getEntityClass().getDeclaredFields();
		updateFieldList = new ArrayList<Method>(fs.length);
		int appendTimes = 0;
		for(int i = 0; i < fs.length; i++) {
			//没有get，set方法忽略
			if(!MethodUtil.hasGetterAndSetter(this.getEntityClass(), fs[i])
					|| fs[i] .equals( this.idField )) {
				continue;
			}
			try {
				updateFieldList.add(MethodUtil.getGetterMethod(entityClass, 
						MethodUtil.getGetterMethodName(fs[i].getName(), 
								(fs[i].getType() == Boolean.class || fs[i].getType() == boolean.class))));
			} catch (Exception e) {
				updateSQL = null;
				updateFieldList = null;
				throw new IllegalStateException("No getter method for field " + entityClass.getName() + "." + fs[i].getName(), e);
			}
			if(appendTimes > 0) {
				updateSQL.append(",\n");
			}
			Column col = fs[i].getAnnotation(Column.class);
			updateSQL.append("\ta.").append(null == col ? fs[i].getName() : col.name()).append("=?");
			appendTimes++;
		}
		updateSQL.append("\n");
		Column idColumn = this.idField.getAnnotation(Column.class);
		updateSQL.append("where a.").append(null == idColumn ? this.idField.getName() : idColumn.name()).append("=?");
		updateFieldList.add(MethodUtil.getGetterMethod(entityClass, MethodUtil.getGetterMethodName(this.idField.getName())));
		return updateSQL.toString();
	}
	
	
	public List<Method> getInsertSQLGetterMethodList(Object entity) throws Exception {
		if(null == insertFieldList) {
			createInsertSQL(entity);
		}
		return insertFieldList;
	}
	public List<Method> getUpdateSQLGetterMethodList(Object entity) throws Exception {
		if(null == updateFieldList) {
			createUpdateSQL(entity);
		}
		return updateFieldList;
	}
	
}
