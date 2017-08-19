package com.hty.partition.dao.impl;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.QueryException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;

import com.hty.partition.PartitionManager;
import com.hty.partition.dao.BaseDao;
import com.hty.partition.exception.PartitionException;
import com.hty.partition.register.PartitionEntity;
import com.hty.partition.util.FieldMappingUtil;
import com.hty.partition.util.HibernateConnectionHelper;

public class HibernateBaseDaoImpl implements BaseDao {

	private Log logger = LogFactory.getLog(getClass());
	
	private SessionFactory sessionFactory;
	
	public HibernateBaseDaoImpl(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}
	
	private Session getSession() {
		Session session = sessionFactory.getCurrentSession();
		if(null == session) {
			session = sessionFactory.openSession();
		}
		return session;
	}
	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}
	
	public Connection getConnection() {
		HibernateConnectionHelper work = new HibernateConnectionHelper();
		getSession().doWork(work);
		return work.getConnection();
	}
	
	public List<Map<String, Object>> executeSQLQuery(String sql, Object... params) 
			throws Exception {
		Connection conn = getConnection();
		PreparedStatement state = conn.prepareStatement(sql);
		try {
			List<Map<String, Object>> list = null;
			setPrepareStatementParameters(state, params);
			ResultSet rs = state.executeQuery();
			ResultSetMetaData md = rs.getMetaData();
			int columncount = md.getColumnCount();
			rs.beforeFirst();
			while(rs.next()) {
				if(null == list) {
					list = new ArrayList<Map<String, Object>>();
				}
				Map<String, Object> ret = new HashMap<String, Object>();
				for(int i=1;i<=columncount;i++) {
					ret.put(md.getColumnLabel(i), rs.getObject(i));
				}
				list.add(ret);
			}
			return list;
		} catch (SQLException e) {
			throw e;
		} finally {
			try { state.close(); } catch (Exception e) { }
		}
	}
	

	public int executeSQLUpdate(String sql, Object... params) throws Exception {
		Connection conn = getConnection();
		PreparedStatement state = conn.prepareStatement(sql);
		setPrepareStatementParameters(state, params);
		int updateCount = state.executeUpdate();
		state.close();
		return updateCount;
	}
	
	public boolean saveObject(Object entity) throws Exception {
		PartitionEntity<?> partitionEntity = PartitionManager.getPartitionEntity(entity.getClass());
		if(null == partitionEntity) {
			throw new PartitionException("No PartitionEntitiy mapped for class " + entity.getClass() + ".");
		}
		String insertSQL = partitionEntity.createInsertSQL(entity);
		List<Method> ms = partitionEntity.getInsertSQLGetterMethodList(entity);
		List<Object> params = new ArrayList<Object>();
		if(null != ms && ms.size() > 0) {
			for(Method m : ms) {
				params.add(m.invoke(entity));
			}
		}
		logger.info("SQL :\n" + insertSQL);
		Connection conn = getConnection();
		PreparedStatement state = conn.prepareStatement(insertSQL);
		setPrepareStatementParameters(state, params.toArray());
		int updateCount = state.executeUpdate();
		state.close();
		return updateCount > 0;
	}
	
	public boolean saveObjectList(List<Object> entities) throws Exception {
		if(null != entities) {
			boolean success = true;;
			for(Object o : entities) {
				success = success && this.saveObject(o);
			}
			return success;
		}
		return true;
	}
	

	public boolean updateObject(Object entity) throws Exception {
		PartitionEntity<?> partitionEntity = PartitionManager.getPartitionEntity(entity.getClass());
		if(null == partitionEntity) {
			throw new PartitionException("No PartitionEntitiy mapped for class " + entity.getClass() + ".");
		}
		String updateSQL = partitionEntity.createUpdateSQL(entity);
		List<Method> ms = partitionEntity.getUpdateSQLGetterMethodList(entity);
		List<Object> params = new ArrayList<Object>();
		if(null != ms && ms.size() > 0) {
			for(Method m : ms) {
				params.add(m.invoke(entity));
			}
		}
		logger.info("SQL :\n" + updateSQL);
		Connection conn = getConnection();
		PreparedStatement state = conn.prepareStatement(updateSQL);
		setPrepareStatementParameters(state, params.toArray());
		int updateCount = state.executeUpdate();
		state.close();
		return updateCount > 0;
	}
	
	public <T> T executeUniqueSQLQuery(Class<T> clazz, String sql,
			Object... params) throws Exception {
		T item = null;
		Connection connection = getConnection();
		PreparedStatement state = connection.prepareStatement(sql);
		try {
			setPrepareStatementParameters(state, params);
			ResultSet rs = state.executeQuery();
			ResultSetMetaData md = rs.getMetaData();
			int columncount = md.getColumnCount();
			rs.beforeFirst();
			Map<String, Field> mapping = FieldMappingUtil.getEntityMapping(clazz);
			if(rs.next()) {
				item = clazz.newInstance();
				for(int i=1;i<=columncount;i++) {
					Field ff = mapping.get(md.getColumnLabel(i).toLowerCase());
					if(null == ff)
						continue;
					//apache BeanUtils处理空值报错，此处做特殊处理
					Object value = rs.getObject(i);
					if(null == value && !isSimpleType (ff.getType())) {
						try {
							ff.setAccessible(true);
							ff.set(item, null);
							ff.setAccessible(false);
						} catch (Exception e) { 
							logger.error("Error setting property " + clazz.getName() + "." + ff.getName() + " with null value.");
							e.printStackTrace(); 
						}
					} 
					else {
						BeanUtils.setProperty(item, ff.getName(), rs.getObject(i));
					}
				}
				if(rs.next()) {
					throw new QueryException("Query return no uniuqe result!");
				}
			}
			return item;
		} catch (SQLException e) {
			throw e;
		} finally {
			try { state.close(); } catch (Exception e) { }
		}
	}


	public <T> List<T> executeSQLQuery(Class<T> clazz, String sql,
			Object... params) throws Exception {
		Connection connection = getConnection();
		PreparedStatement state = connection.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		try {
			setPrepareStatementParameters(state, params);
			ResultSet rs = state.executeQuery();
			ResultSetMetaData md = rs.getMetaData();
			int columncount = md.getColumnCount();
			rs.beforeFirst();
			Map<String, Field> mapping = FieldMappingUtil.getEntityMapping(clazz);
			List<T> queryResult = null;
			while(rs.next()) {
				if(null == queryResult) {
					queryResult = new ArrayList<T>();
				}
				T item = (T) clazz.newInstance();
				for(int i=1;i<=columncount;i++) {
					Field ff = mapping.get(md.getColumnLabel(i).toLowerCase());
					if(null == ff)
						continue;
					//apache BeanUtils处理空值报错，此处做特殊处理
					Object value = rs.getObject(i);
					if(null == value && !isSimpleType (ff.getType())) {
						try {
							ff.setAccessible(true);
							ff.set(item, null);
							ff.setAccessible(false);
						} catch (Exception e) {
							logger.error("Error setting property " + clazz.getName() + "." + ff.getName() + " with null value.");
							e.printStackTrace(); 
						}
					} 
					else {
						BeanUtils.setProperty(item, ff.getName(), value);
					}
				}
				queryResult.add(item);
			}
			return queryResult;
		} catch (Exception e) {
			throw e;
		} finally {
			try { state.close(); } catch (Exception e) { }
		}
	}

	public Map<String, Object> executeUniqueSQLQuery(String sql,
			Object... params) throws Exception {
		Connection connection = getConnection();
		PreparedStatement state = connection.prepareStatement(sql);
		try {
			setPrepareStatementParameters(state, params);
			ResultSet rs = state.executeQuery();
			ResultSetMetaData md = rs.getMetaData();
			int columncount = md.getColumnCount();
			rs.beforeFirst();
			Map<String, Object> ret = null;
			if(rs.next()) {
				if(null == ret) {
					ret = new HashMap<String, Object>();
				}
				for(int i=1;i<=columncount;i++) {
					ret.put(md.getColumnLabel(i), rs.getObject(i));
				}
				if(rs.next()) {
					throw new QueryException("Query return no uniuqe result!");	
				}
			}
			return ret;
		} catch (Exception e) {
			throw e;
		} finally {
			try { state.close(); } catch (Exception e) { }
		}
	}
	
	public long getCount(String sql, Object... params) throws Exception {
		Connection connection = getConnection();
		PreparedStatement state = connection.prepareStatement(sql);
		try {
			setPrepareStatementParameters(state, params);
			ResultSet rs = state.executeQuery();
			rs.beforeFirst();
			if(rs.next()) {
				return (Long) rs.getObject(1);
			}
			return 0;
		} catch (Exception e) {
			throw e;
		} finally {
			try { state.close(); } catch (Exception e) { }
		}
	}

	public boolean exists(String sql, Object... params) throws Exception {
		Connection connection = getConnection();
		PreparedStatement state = connection.prepareStatement(sql);
		try {
			setPrepareStatementParameters(state, params);
			ResultSet rs = state.executeQuery();
			rs.beforeFirst();
			if(rs.next()) {
				return true;
			}
			return false;
		} catch (Exception e) {
			throw e;
		} finally {
			try { state.close(); } catch (Exception e) { }
		}
	}

	public Object getSingleCellData(String sql, Object... params)
			throws Exception {
		Connection connection = getConnection();
		PreparedStatement state = connection.prepareStatement(sql);
		try {
			setPrepareStatementParameters(state, params);
			ResultSet rs = state.executeQuery();
			rs.beforeFirst();
			if(rs.next()) {
				return rs.getObject(1);
			}
			return null;
		} catch (Exception e) {
			throw e;
		} finally {
			try { state.close(); } catch (Exception e) { }
		}
	}
	
	
	/**
	 * 为PreparedStatement赋值
	 * @param state
	 * @param values
	 * @throws SQLException
	 */
	private void setPrepareStatementParameters(PreparedStatement state, Object... values) 
			throws SQLException {
		if(null == values || values.length == 0) {
			return;
		}
		for(int i = 1; i <= values.length; i++) {
			Object v = values[i - 1];
			if(null == v) {
				state.setNull(i, Types.CHAR);
			} else if(v instanceof String) {
				state.setString(i, (String) v);
			} else if(v instanceof Integer) {
				state.setInt(i, (Integer) v);
			} else if(v instanceof Long) {
				state.setLong(i, (Long) v);
			} else if(v instanceof Double) {
				state.setDouble(i, (Double) v);
			} else if(v instanceof Short) {
				state.setShort(i, (Short) v);
			} else if(v instanceof Byte) {
				state.setByte(i, (Byte) v);
			} else if(v instanceof Float) {
				state.setFloat(i, (Float) v);
			} else if(v instanceof Boolean) {
				state.setBoolean(i, (Boolean) v);
			} else if(v instanceof Blob) {
				state.setBlob(i, (Blob) v);
			} else if(v instanceof Clob) {
				state.setClob(i, (Clob) v);
			} else if(v instanceof Time) {
				state.setTime(i, (Time) v);
			} else if(v instanceof java.sql.Date) {
				state.setDate(i, (java.sql.Date) v);
			} else if(v instanceof Timestamp) {
				state.setTimestamp(i, (Timestamp) v);
			} else if(v instanceof java.util.Date) {
				state.setDate(i, new java.sql.Date(((java.util.Date) v).getTime()) );
			} else if(v instanceof Calendar) {
				state.setDate(i, new java.sql.Date(((Calendar)v).getTimeInMillis()) );
			} else if(v instanceof BigDecimal) {
				state.setBigDecimal(i, (BigDecimal) v);
			} else {
				state.setObject(i, v);
			}
		}
	}
	/**
	 * 判断是否简单数据类型
	 * @param clazz
	 * @return
	 */
	private boolean isSimpleType (Class<?> clazz) {
		if (int.class == clazz || long.class == clazz 
				|| short.class == clazz || byte.class == clazz
				|| char.class == clazz || boolean.class == clazz
				|| float.class == clazz || double.class == clazz )
			return true;
		return false;
	}

}
