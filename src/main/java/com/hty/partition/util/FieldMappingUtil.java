package com.hty.partition.util;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.Column;

/**
 * 将数据库字段和类字段映射的工具类
 * @author Tisnyi
 */
public class FieldMappingUtil {
	/** 映射数据集合 */
	private static final Map<Class<?>, Map<String, Field>> mappings = 
			new HashMap<Class<?>, Map<String, Field>>();
	
	/**
	 * 添加映射关系
	 * @param clazz
	 * @param mapping
	 */
	private static synchronized void addMapping(Class<?> clazz, Map<String, Field> mapping) {
		if(!mappings.containsKey(clazz)) {
			mappings.put(clazz, mapping);
		}
	}
	/**
	 * 生成映射
	 * @param clazz
	 */
	private static Map<String, Field> mapClazz(Class<?> clazz) {
		Map<String, Field> mapping = new HashMap<String, Field>();
		Field[] fields = clazz.getDeclaredFields();
		for(Field field : fields) {
			Column ano = field.getAnnotation(Column.class);
			if(null != ano){
				String col_name = ano.name();
				mapping.put(col_name.toLowerCase(), field);
			}else{
				mapping.put(field.getName().toLowerCase(), field);
			}
		}
		addMapping(clazz, mapping);
		return mapping;
	}
	
	
	/**
	 * 获取类的映射信息
	 * @param clazz
	 * @return
	 */
	public static Map<String, Field> getEntityMapping(Class<?> clazz) {
		Map<String, Field> mapping = mappings.get(clazz);
		if(null == mapping) {
			mapping = mapClazz(clazz);
		}
		return mapping;
	}
}
