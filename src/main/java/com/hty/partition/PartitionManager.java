package com.hty.partition;

import java.util.HashMap;
import java.util.Map;

import com.hty.partition.register.PartitionEntity;
/**
 * 管理注册的PartitionEntity
 * @author Hetianyi
 */
public class PartitionManager {
	
	private static Map<String, Integer> entityviews = new HashMap<String, Integer>();
	
	private static Map<String, PartitionEntity<?>> entitiesViewMap = 
			new HashMap<String, PartitionEntity<?>>();
	
	private static Map<Class<?>, PartitionEntity<?>> entitiesMap = 
			new HashMap<Class<?>, PartitionEntity<?>>();
	/**
	 * 添加一个PartitionEntity
	 * @param partitionEntity
	 */
	public static void addEntityView(PartitionEntity<?> partitionEntity) {
		entityviews.put(partitionEntity.getViewName(), partitionEntity.getPartitionCount());
		entitiesMap.put(partitionEntity.getEntityClass(), partitionEntity);
		entitiesViewMap.put(partitionEntity.getViewName(), partitionEntity);
	}
	/**
	 * 根据视图名查询PartitionEntity是否存在
	 * @param viewName 视图名
	 * @return
	 */
	public static boolean containsPartitionView(String viewName) {
		return entityviews.containsKey(viewName);
	}
	/**
	 * 根据EntityClass查询PartitionEntity是否存在
	 * @param entityClass EntityClass
	 * @return
	 */
	public static boolean containsPartitionEntity(Class<?> entityClass) {
		return entitiesMap.containsKey(entityClass);
	}
	/**
	 * 根据视图名查询PartitionEntity的分区数
	 * @param viewName 视图名
	 * @return
	 */
	public static int getPartitionCount(String viewName) {
		Integer count = entityviews.get(viewName);
		return null == count ? 0 : count;
	}
	/**
	 * 根据EntityClass获取PartitionEntity
	 * @param clazz EntityClass
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> PartitionEntity<T> getPartitionEntity(Class<T> clazz) {
		return (PartitionEntity<T>) entitiesMap.get(clazz);
	}
	/**
	 * 根据视图名获取PartitionEntity
	 * @param viewName 视图名
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> PartitionEntity<T> getPartitionEntity(String viewName) {
		viewName = null == viewName ? "" : viewName.toLowerCase();
		return (PartitionEntity<T>) entitiesViewMap.get(viewName);
	}
	
}
