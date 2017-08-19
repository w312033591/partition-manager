package com.hty.partition.dao;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

import com.hty.partition.annotation.DaoMethod;

/**
 * 使用 partition-manager 需要使用 partition-manager 提供的DAO层来实现数据操作。<br>
 * 此DAO层主要是帮助分表函数变量自动替换为真实的分表名。
 * 同时在书写SQL语句时有一些语法规范需要遵循：<br>
 * <ul>
 * <li>已知业务ID，根据ID查询对象时的SQL写法：<br><br>
 * <code>
 * String sql = "select * from $one(t_order, ?) a where a.id=?";
 * </code><br><br>
 * 调用DAO方法传入参数时要将决定分表的路由函数$one(t_order, ?)的参数传入:<br><br>
 * <code>
 * dao.executeUniqueSQLQuery(sql, trueid, trueid);
 * </code><br><br>
 * </li>
 * <li>
 * 未知业务ID，根据业务围绕的服务对象ID查询业务对象列表。<br>
 * 例如，已知用户ID查询用户的订单列表；已知论坛的分区ID，查询分区的主题列表等。<br>
 * SQL写法：<br><br>
 * String sql = "select * from $list(t_order, ?) a where a.userid = ? limit 0, 10"<br><br>
 * 调用DAO方法传入参数时要将决定分表的路由函数$list(t_order, ?)的参数传入:<br><br>
 * <code>
 * dao.executeSQLQuery(Order.class, sql, userid, userid);
 * </code><br><br>
 * </li>
 * <li>
 * 查询条件既没有"业务对象ID"也没有业务围绕的"服务对象ID"(但是结果必须设置分页大小)。<br>
 * 此时DAO层将查询语句分发至每个分表查询，然后将结果合并。(必要时，对查询列建立索引)<br>
 * <i>重要：该情况下每个SQL语句中至多只能出现一个路由函数$(table)</i><br>
 * 例如，按照日期查询订单列表：<br><br>
 * <code>
 * select * from $(t_order) a order by a.create_date desc limit 0, 10
 * </code><br><br>
 * BaseDao会将SQL解析为：
 * <pre><code>
 * select * from (
	(select * from t_order_00 a ORDER BY a.create_date desc LIMIT 0,10)
	UNION
	(select * from t_order_01 a ORDER BY a.create_date desc LIMIT 0,10)
	UNION
	(select * from t_order_02 a ORDER BY a.create_date desc LIMIT 0,10)
	UNION
	(select * from t_order_03 a ORDER BY a.create_date desc LIMIT 0,10)
	UNION
	(select * from t_order_04 a ORDER BY a.create_date desc LIMIT 0,10)
	UNION
	(select * from t_order_05 a ORDER BY a.create_date desc LIMIT 0,10)
	UNION
	(select * from t_order_06 a ORDER BY a.create_date desc LIMIT 0,10)
	UNION
	(select * from t_order_07 a ORDER BY a.create_date desc LIMIT 0,10)
	UNION
	(select * from t_order_08 a ORDER BY a.create_date desc LIMIT 0,10)
	UNION
	(select * from t_order_09 a ORDER BY a.create_date desc LIMIT 0,10)
 *) un ORDER BY un.create_date desc LIMIT 0,10
 * </code></pre>
 * </li>
 * </ul>
 * @author Tisnyi 2017/08/16
 * @version 1.0
 */
public interface BaseDao {
	/**
	 * 实现BaseDao需要提供获取数据库连接的接口实现。
	 * @return 数据库连接：java.sql.Connection
	 */
	abstract Connection getConnection();
	/**
	 * 保存一个经注册的对象，由于对象ID已提前生成，所以返回
	 * @param entity 保存的对象
	 * @return 是否插入成功
	 * @throws Exception
	 */
	public abstract boolean saveObject(Object entity) throws Exception;
	/**
	 * 更新一个经注册的对象
	 * @param entity 更新的对象
	 * @return 是否更新成功
	 * @throws Exception
	 */
	public abstract boolean updateObject(Object entity) throws Exception;
	/**
	 * 提供此接口主要是为了在批量保存时减少调用次数。
	 * @param entity
	 * @return
	 * @throws Exception
	 */
	public abstract boolean saveObjectList(List<Object> entities) throws Exception;
	/**
	 * 查询，返回结果类型为：List&lt;Map&lt;String, Object&gt;&gt;<br>
	 * 每行数据作为一个Map对象，每行的字段名作为key，字段值作为value。
	 * @param sql 查询SQL
	 * @param params 查询条件参数
	 * @return List&lt;Map&lt;String, Object&gt;&gt;
	 * @throws Exception
	 */
	@DaoMethod(sqlpos = 0, argspos = 1)
	public abstract List<Map<String, Object>> executeSQLQuery(String sql, Object... params) throws Exception;
	/**
	 * 执行数据库更新操作
	 * @param sql 查询SQL
	 * @param params 查询条件参数
	 * @return 更新的行数
	 * @throws Exception
	 */
	@DaoMethod(sqlpos = 0, argspos = 1)
	public abstract int executeSQLUpdate(String sql, Object... params) throws Exception;
	/**
	 * 从数据库查询一条数据并封装为一个Java对象。
	 * @param clazz 封装对象的类
	 * @param sql 查询SQL
	 * @param params 查询条件参数
	 * @return T
	 * @throws Exception
	 */
	@DaoMethod(sqlpos = 1, argspos = 2)
	public abstract <T> T executeUniqueSQLQuery(Class<T> clazz, String sql, Object... params) throws Exception;
	/**
	 * 从数据库查询一批条数据并封装为一个Java对象列表。
	 * @param clazz 封装类
	 * @param sql 查询SQL
	 * @param params 查询条件参数
	 * @return
	 * @throws Exception
	 */
	@DaoMethod(sqlpos = 1, argspos = 2)
	public abstract <T> List<T> executeSQLQuery(Class<T> clazz, String sql, Object... params) throws Exception;
	
	/**
	 * 执行原生SQL查询，返回Map集合
	 * @param sql 查询SQL
	 * @param params 查询条件参数
	 * @return
	 * @throws Exception
	 */
	@DaoMethod(sqlpos = 0, argspos = 1)
	public Map<String, Object> executeUniqueSQLQuery(String sql, Object... params) throws Exception;
	
	/**
	 * 获取结果集数，推荐书写的sql形式类似：<br>
	 * select count(*) from table where xxx<br>
	 * 取结果集的第一行第一列作为返回值，
	 * @param sql 查询语句
	 * @param params 参数
	 * @return
	 * @throws Exception
	 */
	@DaoMethod(sqlpos = 0, argspos = 1)
	public long getCount(String sql, Object... params) throws Exception;
	/**
	 * 查询结果是否存在，推荐书写的sql形式类似：<br>
	 * select 1 from table where xxx
	 * @param sql 查询语句
	 * @param params 参数
	 * @return 存在:true，不存在:false，此过程调用{@link #getCount}
	 * @throws Exception
	 */
	@DaoMethod(sqlpos = 0, argspos = 1)
	public boolean exists(String sql, Object... params) throws Exception;
	/**
	 * 获取元数据并直接返回（不放在Map中返回），方便用户获取数据，不用手动从Map中get。<br>
	 * 注意：请确保查询的返回值之多是一行一列。<br>
	 * @param sql 查询语句
	 * @param params 参数
	 * @return 元数据对象，需要自行转换类型
	 * @throws Exception
	 */
	@DaoMethod(sqlpos = 0, argspos = 1)
	public Object getSingleCellData(String sql, Object... params) throws Exception;
}
