package com.hty.partition.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
/**
 * 注解DAO层方法，标注SQL和SQL参数的位置
 * @author Hetianyi
 */
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface DaoMethod {
	/** DAO方法的SQL位置 */
	public int sqlpos();
	/** DAO方法的SQL参数位置 */
	public int argspos();
}
