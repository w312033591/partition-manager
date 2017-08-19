package com.hty.partition.dao;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.SessionFactory;

import com.hty.partition.PartitionManager;
import com.hty.partition.annotation.DaoMethod;
import com.hty.partition.bean.MatchRange;
import com.hty.partition.dao.impl.HibernateBaseDaoImpl;
import com.hty.partition.exception.IllegalSqlException;
import com.hty.partition.exception.PartitionException;
import com.hty.partition.register.PartitionEntity;
import com.hty.partition.util.StringLenFixUtil;

/**
 * 该类处理代理接口类BaseDao的sql分表转换工作。
 * @author Hetianyi
 */
public class BaseDaoInvocationHandler implements InvocationHandler {
	private Log logger = LogFactory.getLog(getClass());
	private BaseDao dao = null;
	private final String pattern_one = "\\$one\\(([^\\?]+),(\\s)*\\?\\)";
	private final String pattern_list = "\\$list\\(([^\\?]+),(\\s)*\\?\\)";
	private final String pattern_other = "\\$\\(([^\\?]+)\\)";
	private final Pattern pq = Pattern.compile("\\?");
	private final Pattern p_one = Pattern.compile(pattern_one);
	private final Pattern p_list = Pattern.compile(pattern_list);
	private final Pattern p_other = Pattern.compile(pattern_other);
	
	public BaseDaoInvocationHandler(int a, SessionFactory sessionFactory) {
		this.dao = new HibernateBaseDaoImpl(sessionFactory);
	}
	
	@SuppressWarnings("unchecked")
	public Object invoke(Object proxy, Method method, Object[] args)
			throws Throwable {
		DaoMethod annotation = method.getAnnotation(DaoMethod.class);
		if(null != annotation) {
			int sqlpos = annotation.sqlpos();
			int argspos = annotation.argspos();
			if(null != args && args.length-1 >= argspos) {
				Object[] sqlArgs = (Object[]) args[argspos];
				String sql = (String) args[sqlpos];
				if(!isEmpty(sql)) {
					//跳过参数位置的表视图名称
					Set<Integer> skippedSQLArgPos = new HashSet<Integer>();
					//记录占位符所在位置
					List<Integer> placeholderPos = new ArrayList<Integer>();
					//SQL参数（过滤的辅助参数）
					List<Object> newSQLArgs = new ArrayList<Object>();
					List<Object> newMethodArgs = new ArrayList<Object>();
					for(Object arg : args) {
						newMethodArgs.add(arg);
					}
					//查找‘?’所在位置.
					placeholderPos = filterPlaceHolderPositions(sql);
					//替换partition-one函数为分表名
					sql = filterOneFunction(sql, placeholderPos, sqlArgs, skippedSQLArgPos);
					//替换partition-list函数为分表名
					sql = filterListFunction(sql, placeholderPos, sqlArgs, skippedSQLArgPos);
					for(int i = 0; i < sqlArgs.length; i++) {
						if(!skippedSQLArgPos.contains(i)) {
							newSQLArgs.add(sqlArgs[i]);
						}
					}
					//将$(table)函数分发到分表
					List<Object> handleResult = filterOtherFunction(sql, newSQLArgs);
					sql = (String) handleResult.get(1);
					newSQLArgs = (List<Object>) handleResult.get(0);
					
					newMethodArgs.set(sqlpos, sql);
					newMethodArgs.set(argspos, newSQLArgs.toArray());
					logger.info("SQL :\n" + sql);
					return method.invoke(this.dao, newMethodArgs.toArray());
				}
			}
		}
		return method.invoke(this.dao, args);
	}
	/**
	 * 查找‘?’所在位置.
	 * @param sql
	 * @return
	 */
	private List<Integer> filterPlaceHolderPositions(String sql) {
		List<Integer> placeholderPos = new ArrayList<Integer>();
		Matcher m = pq.matcher(sql);
		while(m.find()) {
			placeholderPos.add(m.start());
		}
		return placeholderPos;
	}
	/**
	 * 查找partition-one函数位置范围($one(t_order, ?))
	 * @param sql
	 * @param placeholderPos
	 * @param sqlArgs
	 * @param skippedSQLArgPos
	 * @return
	 * @throws PartitionException
	 */
	private String filterOneFunction(String sql, 
			List<Integer> placeholderPos, 
			Object[] sqlArgs,
			Set<Integer> skippedSQLArgPos) throws PartitionException {
		Matcher m = p_one.matcher(sql);
		String partitionView = null;
		List<MatchRange> ranges = new ArrayList<MatchRange>();;
		while (m.find()) {
			partitionView = m.group(1).trim();
			logger.info("table view : " + partitionView);
			int mstart = m.start(), mend = m.end();
			//比对，过滤掉partition函数的参数
			for(int i = 0; i < placeholderPos.size(); i++) {
				Integer q = placeholderPos.get(i);
				if(q >= mstart && q < mend) {
					skippedSQLArgPos.add(i);
					PartitionEntity<?> entitiy = PartitionManager.getPartitionEntity(partitionView);
					if(null == entitiy) {
						throw new PartitionException("No PartitionEntity mapped for table \"" + partitionView + "\".");
					}
					if(sqlArgs.length <= i) {
						throw new IllegalArgumentException("No enought SQL parameter : \n\t" + sql);
					}
					if(null == sqlArgs[i] || sqlArgs[i].getClass() != String.class) {
						throw new IllegalArgumentException("Expect String type id to decide partition table but got parameter which is null or not a Strng type.");
					}
					String businessId = (String) sqlArgs[i];
					String trueTable = entitiy.translatePartitionTable(businessId);
					MatchRange range = new MatchRange(mstart, mend, trueTable);
					ranges.add(range);
					break;
				}
			}
		}
		for(int i = ranges.size() - 1; i >= 0; i--) {
			sql = replaceRange2ViewName(sql, ranges.get(i));
		}
		return sql;
	}
	/**
	 * 查找partition-list函数位置范围($list(t_order, ?))
	 * @param sql
	 * @param placeholderPos
	 * @param sqlArgs
	 * @param skippedSQLArgPos
	 * @return
	 * @throws PartitionException
	 */
	private String filterListFunction(String sql, 
			List<Integer> placeholderPos, 
			Object[] sqlArgs,
			Set<Integer> skippedSQLArgPos) throws PartitionException {
		//查找partition-one函数位置范围
		String partitionView = null;
		List<MatchRange> ranges = new ArrayList<MatchRange>();;
		Matcher m = p_list.matcher(sql);
		while (m.find()) {
			partitionView = m.group(1).trim();
			logger.info("table view : " + partitionView);
			int mstart = m.start(), mend = m.end();
			//比对，过滤掉partition函数的参数
			for(int i = 0; i < placeholderPos.size(); i++) {
				Integer q = placeholderPos.get(i);
				if(q >= mstart && q < mend) {
					skippedSQLArgPos.add(i);
					PartitionEntity<?> entitiy = PartitionManager.getPartitionEntity(partitionView);
					if(null == entitiy) {
						throw new PartitionException("No PartitionEntity mapped for table \"" + partitionView + "\".");
					}
					if(sqlArgs.length <= i) {
						throw new IllegalArgumentException("No enought SQL parameter : \n\t" + sql);
					}
					if(null == sqlArgs[i] || !isNumberType(sqlArgs[i].getClass())) {
						throw new IllegalArgumentException("Expect Number[long or int] type to decide partition table but got parameter which is null or not a Number type.");
					}
					Long centerId = Long.valueOf(String.valueOf(sqlArgs[i]));
					String trueTable = entitiy.translatePartitionTable(centerId);
					MatchRange range = new MatchRange(mstart, mend, trueTable);
					ranges.add(range);
					break;
				}
			}
		}
		for(int i = ranges.size() - 1; i >= 0; i--) {
			sql = replaceRange2ViewName(sql, ranges.get(i));
		}
		return sql;
	}
	
	/**
	 * 查找partition-list函数位置范围($(t_order, ?))
	 * @param sql
	 * @param placeholderPos
	 * @param sqlArgs
	 * @param skippedSQLArgPos
	 * @return
	 * @throws PartitionException
	 */
	private List<Object> filterOtherFunction(String sql, List<Object> sqlArgs) throws PartitionException {
		//查找partition-one函数位置范围
		String partitionView = null;
		boolean appears = false;
		PartitionEntity<?> entitiy = null;
		Matcher m = p_other.matcher(sql);
		List<Object> handleResult = new ArrayList<Object>(2);
		while (m.find()) {
			//路由函数只能出现一次
			if(appears) {
				throw new IllegalSqlException("Router function $(sometable) can only appears once in SQL statement.");
			}
			partitionView = m.group(1).trim();
			logger.info("table view : " + partitionView);
			int mstart = m.start();
			entitiy = PartitionManager.getPartitionEntity(partitionView);
			if(null == entitiy) {
				throw new PartitionException("No PartitionEntity mapped for table \"" + partitionView + "\".");
			}
			//检查路由函数前面的SQL占位符?出现次数，将SQL参数分割。
			List<Object> newArgs = new ArrayList<Object>();
			List<Integer> beforeQPos = filterPlaceHolderPositions(sql.substring(0, mstart));
			for(int i = 0; i < beforeQPos.size(); i++) {
				newArgs.add(sqlArgs.get(i));
			}
			for(int i = 0; i < entitiy.getPartitionCount(); i++) {
				for(int k = 0; k < sqlArgs.size(); k++) {
					newArgs.add(sqlArgs.get(k));
				}
			}
			for(int i = beforeQPos.size(); i < sqlArgs.size(); i++) {
				newArgs.add(sqlArgs.get(i));
			}
			handleResult.add(newArgs);
			appears = true;
		}
		if(appears) {
			StringBuilder sb = new StringBuilder();
			sb.append("(\n");
			for(int i = 0; i < entitiy.getPartitionCount(); i++) {
				if(i != 0) {
					sb.append("\n\tUNION\n");
				}
				sb.append("\t(");
				sb.append(m.replaceAll(partitionView + "_" + StringLenFixUtil.fixLength(i, 2)));
				sb.append(")");
			}
			sb.append("\n)");
			sql = m.replaceAll(sb.toString());
		} else {
			handleResult.add(sqlArgs);
		}
		handleResult.add(sql);
		return handleResult;
	}
	
	
	/**
	 * 将匹配范围的函数替换为分表名
	 * @param sql
	 * @param viewName
	 * @param start
	 * @param end
	 * @return
	 */
	private String replaceRange2ViewName(String sql, MatchRange range) {
		return sql.substring(0, range.getStart()) + range.getTrueTable() + sql.substring(range.getEnd());
	}
	
	private boolean isEmpty(String sql) {
		return null == sql || "".equals(sql.trim());
	}
	/**
	 * 检测ID是否整数字类型
	 * @param clazz
	 * @return
	 */
	private boolean isNumberType(Class<?> clazz) {
		return clazz == Long.class || 
				clazz == long.class || 
				clazz == Integer.class || 
				clazz == int.class
				;
	}
}
