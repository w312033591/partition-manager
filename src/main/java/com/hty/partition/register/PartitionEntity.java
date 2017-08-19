package com.hty.partition.register;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Id;
import javax.persistence.Table;
import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hty.partition.PartitionManager;
import com.hty.partition.exception.PartitionException;
import com.hty.partition.util.StringLenFixUtil;

/**
 * 分表实体，将一个业务对象拆分为若干个分表。<br>
 * 分表的路由有两种固定的方式：<br><ol>
 * <li>根据订单ID查询订单（返回一条记录），此时必须传入参数订单ID。</li>
 * <li>根据用户查询订单（返回多条记录），此时必须传入用户ID。</li>
 * </ol>
 * 上述例子中订单为需要拆分的业务数据对象，用户和订单紧密关联，用户ID决定了订单的分表ID。<br>
 * 对于符合上述两种关系的业务【业务】均可使用该分表框架。
 * <br><br>
 * 对于业务型的应用，订单围绕用户为中心。
 * 订单由用户产生，建议的做法是这样的：<br><ul>
 * <li>库：在系统用户表将用户关联一个订单数据库ID，这个用户的订单将保存在此数据库服务器上。<br>
 * 如果该库的容量达到阈值，再将用户的库关联信息新增一条指向到新库中。</li><br>
 * <li>表：具体保存到哪个分表，建议使用【数字类型的userid % 分表数量】作为分表ID。<br>
 * 设计的时候数据库的数量要经常增加，但是每个库的分表数量不可改变，否则根据用户ID路由分表将失去精度。<br>
 * 另外，每个用户在每个库中只能使用一张表来保存订单信息。</li>
 * </ul><br>
 * 同时，开发者不用知道分表有多少，不用具体操作某个具体的分表，这些分表对于开发者来说是透明的。<br>
 * 开发者只需要知道这些分表合并的视图名称即可。只要调用响应的PartitionEntity提供的API，<br>
 * 由该类来路由到指定的分表。对于用户-库关联关系建议设计一个专门的关联记录表。<br>
 * 对于论坛类型的应用，帖子围绕分区和用户，不仅需要根据用户能够快速定位主题列表，<br>
 * 而根据分区也要能够快速定位主题列表。
 * 设计思路是：
 * <ul>
 * <li>库：在论坛分区表将分区关联一个数据库ID，这个分区的主题将保存在此数据库服务器上。<br>
 * 在发布主题的同时将用户的主题必要信息平行地冗余到该主题所在数据库的一个映射分表中，该分表ID为【数字类型的userid % 分表数量】</li>
 * <li>分表：将【数字类型的分区ID % 分表数量】作为分表ID，当按照用户为条件查询时，<br>
 * 指令下发到每个数据库对应的用户专用冗余主题信息的分表，每个数据库查询出分页大小条数据，然后在web端进行合并过滤。</li>
 * </ul>
 * @author Hetianyi
 * @param <T> 
 */
public abstract class PartitionEntity<T> {
	private Log logger = LogFactory.getLog(getClass());
	/** 拆分的业务对象Class */
	protected final Class<T> entityClass;
	/** 业务对象拆分的数量（分表数量），形如xxx_00,xxx_01,xxx_02... */
	protected final int partitionCount;
	/** 根据业务对象的ID路由找到数据所在表的路由类 */
	protected final PartitionRouter router;
	/** 映射业务ID的分表ID信息和实际表名 */
	protected final Map<Integer, String> partitionIdMapTable = new HashMap<Integer, String>();
	/** 拆分的业务对象视图名称 */
	protected String viewName;
	/** 拆分的业务对象的ID字段 */
	protected Field idField;
	/** 数据源，用于检查和修复分表完整性 */
	protected DataSource dataSource;
	/** 对象映射表的建表语句 */
	protected String create_table_ddl;
	/**
	 * 创建分表对象，该对象必须指定表名(@Table)和ID字段(@Id)且有唯一的ID字段。
	 * @param clazz 映射对象类
	 * @param partitionCount 拆分数量
	 * @param router 路由器
	 * @param dataSource 数据源
	 * @param checkPartitionTables 是否检查和修复分表完整性
	 * @param create_table_ddl 分表建表DDL，需将表名替换为'$TABLE'
	 * @throws Exception
	 */
	public PartitionEntity(Class<T> clazz, int partitionCount, PartitionRouter router, 
			DataSource dataSource, boolean checkPartitionTables, String create_table_ddl) 
			throws Exception {
		if(null == clazz) {
			throw new IllegalArgumentException("Parameter \"clazz\" can not be null.");
		}
		if(partitionCount <= 0) {
			throw new IllegalArgumentException("Parameter \"partitionCount\" must be more than 0.");
		}
		if(null == router) {
			throw new IllegalArgumentException("Parameter \"router\" can not be null.");
		}
		if(checkPartitionTables && null == dataSource) {
			throw new IllegalArgumentException("Parameter \"dataSource\" can not be null.");
		}
		if(checkPartitionTables && null == create_table_ddl) {
			throw new IllegalArgumentException("Parameter \"create_table_ddl\" can not be null.");
		}
		this.entityClass = clazz;
		this.partitionCount = partitionCount;
		this.router = router;
		this.dataSource = dataSource;
		this.create_table_ddl = create_table_ddl;
		initIdMapPartitionTables();
		getIdField();
		PartitionManager.addEntityView(this);
		/** 是否在初始化的时候检查并修复分表完整性 */
		if(checkPartitionTables) {
			checkPartitionTablesAndView();
		}
	}
	
	/**
	 * 将分表数量和分表表名映射
	 */
	private void initIdMapPartitionTables() throws PartitionException {
		Table t = this.entityClass.getAnnotation(Table.class);
		if(null != t && t.name() != null) {
			this.viewName = t.name().toLowerCase();
			for(int i = 0; i < this.partitionCount; i++) {
				logger.info("Mapping partition table[" + t.name() + "_" + StringLenFixUtil.fixLength(i, 2) + "]");
				partitionIdMapTable.put(i, t.name().toLowerCase() + "_" + StringLenFixUtil.fixLength(i, 2));
			}
		} else {
			throw new PartitionException("Entity class has no annotation \"javax.persistence.Table\" or it's name is empty.");
		}
	}
	
	private void getIdField()  throws PartitionException {
		Field[] fs = this.entityClass.getDeclaredFields();
		for(Field f : fs) {
			if(null != f.getAnnotation(Id.class)) {
				this.idField = f;
				break;
			}
		}
		if(null == this.idField || this.idField.getType() != String.class) {
			throw new PartitionException("No String Id defined in class "+ this.entityClass.getName() +".");
		}
	}
	
	/**
	 * 根据业务对象ID获取分表名
	 * @param id 业务对象ID
	 * @return String 分表名
	 */
	public String translatePartitionTable(String businessId) {
		int tid = router.getPartition(businessId);
		logger.info("Transform table name from businessId:" + businessId + "->" + partitionIdMapTable.get(tid));
		return partitionIdMapTable.get(tid);
	}
	/**
	 * 根据业务服务对象获取分表名
	 * @param serviceObjectId
	 * @return
	 */
	public String translatePartitionTable(Long serviceObjectId) {
		int tid = router.getPartition(serviceObjectId, this.getPartitionCount());
		logger.info("Transform table name from serviceObjectId:" + serviceObjectId + "->" + partitionIdMapTable.get(tid));
		return partitionIdMapTable.get(tid);
	}
	
	
	/**
	 * 1. 检测数据库中的这些分表是否存在是否齐全，不齐全自动创建补齐。<br>
	 * 2. 检测数据库是否存在这些分表的视图，不存在创建
	 * @throws Exception
	 */
	public abstract void checkPartitionTablesAndView() throws Exception;
	/**
	 * 根据entitiy实体类生成新增的SQL
	 * @param entity entitiy实例
	 * @return insert SQL语句
	 * @throws Exception
	 */
	public abstract String createInsertSQL(Object entity) throws Exception;
	/**
	 * 根据entitiy实体类生成更新实体类对应记录的SQL
	 * @param entity entitiy实例
	 * @return update SQL语句
	 * @throws Exception
	 */
	public abstract String createUpdateSQL(Object entity) throws Exception;
	/**
	 * 生成insertSQL时按照字段顺序的getter方法列表
	 * @param entity
	 * @return
	 * @throws Exception
	 */
	public abstract List<Method> getInsertSQLGetterMethodList(Object entity) throws Exception;
	
	/**
	 * 生成updateSQL时按照字段顺序的getter方法列表
	 * @param entity
	 * @return
	 * @throws Exception
	 */
	public abstract List<Method> getUpdateSQLGetterMethodList(Object entity) throws Exception;
	
	
	public Class<T> getEntityClass() {
		return entityClass;
	}
	public int getPartitionCount() {
		return partitionCount;
	}
	public PartitionRouter getRouter() {
		return router;
	}
	public String getViewName() {
		return viewName;
	}
}
