package com.hty.partition.util;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * 方法工具类
 * @author Hetianyi
 */
public class MethodUtil {
	
	private static Map<String, Integer> base2ObjEqualMap = new HashMap<String, Integer>();
	private static Map<String, Integer> obj2BaseEqualMap = new HashMap<String, Integer>();
	
	static{
		base2ObjEqualMap.put(byte.class.getSimpleName(), 1);
		base2ObjEqualMap.put(short.class.getSimpleName(), 2);
		base2ObjEqualMap.put(char.class.getSimpleName(), 3);
		base2ObjEqualMap.put(int.class.getSimpleName(), 4);
		base2ObjEqualMap.put(long.class.getSimpleName(), 5);
		base2ObjEqualMap.put(float.class.getSimpleName(), 6);
		base2ObjEqualMap.put(double.class.getSimpleName(), 7);
		
		obj2BaseEqualMap.put(Byte.class.getSimpleName(), 1);
		obj2BaseEqualMap.put(Short.class.getSimpleName(), 2);
		obj2BaseEqualMap.put(Character.class.getSimpleName(), 3);
		obj2BaseEqualMap.put(Integer.class.getSimpleName(), 4);
		obj2BaseEqualMap.put(Long.class.getSimpleName(), 5);
		obj2BaseEqualMap.put(Float.class.getSimpleName(), 6);
		obj2BaseEqualMap.put(Double.class.getSimpleName(), 7);
		
	}
	
	public static List<Method> match(Class<?> clazz, String name, Object[] args) throws Exception {
		List<Method> matchMethods = getMethod(clazz, name);
		matchMethods = filterMethodByArgs(matchMethods, args);
		return matchMethods;
	}
	
	/**
	 * 根据名称获得所有的方法
	 * @param clazz
	 * @param name
	 * @return
	 * List<Method>
	 */
	public static List<Method> getMethod(Class<?> clazz, String name){
		List<Method> list = new ArrayList<Method>();
		Method[] ms = clazz.getMethods();
		for(Method m : ms){
			if(m.getName().equals(name))
				list.add(m);
		}
		return list;
	}
	/**
	 * 获得类的方法
	 * @param clazz
	 * @param name
	 * @param argTypes
	 * @return
	 * @throws Exception
	 */
	public static Method getMethod(String name, Class<?> clazz, Class<?>... argTypes) throws Exception {
		return clazz.getMethod(name, argTypes);
	}
	/**
	 * 根据返回值过滤方法列表
	 * @param methods
	 * @param returnType
	 * @return
	 */
	public static List<Method> filterMethodByReturnType(List<Method> methods, Class<?> returnType) throws Exception {
		List<Method> tmp = null;
		if(null == methods || methods.size() == 0)
			return tmp;
		tmp = new ArrayList<Method>(methods.size());
		for(Method m : methods){
			if(m.getReturnType() == returnType)
				tmp.add(m);
		}
		return tmp;
	}
	/**
	 * 根据参数类型过滤方法列表
	 * @param methods
	 * @param args
	 * @return
	 */
	public static List<Method> filterMethodByArgs(List<Method> methods, Object[] args) throws Exception{
		//首先比对参数数量The method save(TestCase1.A) is ambiguous for the type TestCase1
		List<Method> tmplist = new ArrayList<Method>();
		List<Integer[]> weights = new ArrayList<Integer[]>();
		boolean allmatch = true;
		for(Method m : methods){
			allmatch = true;
			Class<?>[] pt = m.getParameterTypes();
			//制作关系分布
			Integer[] flidd = new Integer[pt.length];
			boolean isTypeCountMatch = false;
			if((null == pt || pt.length == 0) && (null == args || args.length == 0))
				isTypeCountMatch = true;
			else if((null != pt && null != args) && pt.length == args.length)
				isTypeCountMatch = true;
			if(!isTypeCountMatch)
				continue;
			//分别校验对应位置的参数类型是否匹配
			
			for(int i = 0; i < pt.length; i++){
				if(pt[i] == args[i].getClass() || canPromot(pt[i], args[i].getClass())){
					flidd[i] = 0;
					continue;
				}
				int deep = isSuperClass(pt[i], args[i].getClass());
				if(deep > 0){
					flidd[i] = deep;
				}else{
					allmatch = false;
					break;
				}
			}
			//初步筛选参数数目和类型都匹配的方法，如果此时方法数量依旧大于1，则筛选最近关系的方法
			if(allmatch){
				weights.add(flidd);
				tmplist.add(m);
			}
		}
		//The method ha(A, C) is ambiguous for the type Objtest
		//throw new RuntimeException("The method "+ m.getName() +" is ambiguous for the type " + args[i].getClass());
		//过滤方法，找到最匹配的方法
		//匹配继承关系最近的方法(关系亲疏如果出现交叉则报错)
		if(tmplist.size() < 2)
			return tmplist;
		int[] tmpwei = new int[weights.size()];
		List<Integer> mostclosemethodindexs = new ArrayList<Integer>();
		for(int i=0;i<args.length;i++){
			for(int j=0;j<weights.size();i++){
				tmpwei[j] = weights.get(j)[i];
			}
			//计算关系最近的index,index必须一直保持不变，否则报模糊错
			List<Integer> tmpmostclosemethodindexs = findtheClosest(tmpwei);
			if(i == 0){
				mostclosemethodindexs = tmpmostclosemethodindexs;
				continue;
			}
			if(containslist(tmpmostclosemethodindexs, mostclosemethodindexs)){
				mostclosemethodindexs = tmpmostclosemethodindexs;
			}
		}
		List<Method> finalmethods = new ArrayList<Method>();
		for(int i =0;i<mostclosemethodindexs.size();i++){
			finalmethods.add(tmplist.get(i));
		}
		return finalmethods;
	}
	
	/**
	 * 测试后者是否�芄惶嵘罢�
	 * @param a
	 * @param b
	 * @return
	 */
	public static boolean canPromot(Class<?> target, Class<?> input){
		if((base2ObjEqualMap.containsKey(input.getSimpleName())
				|| obj2BaseEqualMap.containsKey(input.getSimpleName()))
			&& (base2ObjEqualMap.containsKey(target.getSimpleName())
					|| obj2BaseEqualMap.containsKey(target.getSimpleName()))){
			Integer blevel = base2ObjEqualMap.get(input.getSimpleName());
			if(null == blevel){
				blevel = obj2BaseEqualMap.get(input.getSimpleName());
			}
			Integer alevel = base2ObjEqualMap.get(target.getSimpleName());
			if(null == alevel){
				alevel = obj2BaseEqualMap.get(target.getSimpleName());
			}
			if(null == alevel)
				return false;
			if(blevel <= alevel)
				return true;
			else
				return false;
			
		}else
			return false;
	}
	
	/**
	 * 测试后者能否包含前者
	 * @param tmpmostclosemethodindexs
	 * @param mostclosemethodindex
	 * @return
	 */
	public static boolean containslist(List<Integer> tmpmostclosemethodindexs, List<Integer> mostclosemethodindex){
		if(mostclosemethodindex.size() < tmpmostclosemethodindexs.size())
			return false;
		boolean contain = false;
		for(int b : tmpmostclosemethodindexs){
			contain = false;
			for(int a : mostclosemethodindex){
				if(b == a){
					contain = true;
					break;
				}
			}
			if(!contain)
				return false;
		}
		return true;
	}
	public static List<Integer> findtheClosest(int[] sep){
		List<Integer> deplist = new ArrayList<Integer>();
		deplist.add(0);
		int min = sep[0];
		for(int i=1;i<sep.length;i++){
			if(sep[i] < min){
				deplist.clear();
				deplist.add(i);
				min = sep[i];
			}else if(sep[i] == min){
				deplist.add(i);
			}
		}
		return deplist;
	}
	/**
	 * 检测前者是否后者的父类或者间接父类或者实现类
	 * @param obj 检测的对象
	 * @param compared 比较的对象
	 * @return
	 */
	public static int isSuperClass(Class<?> obj, Class<?> compared) {
		int deep = 0;
		if(compared.isInterface() && !obj.isInterface())
			return deep;
		if(compared.isInterface() && obj.isInterface()){
			Class<?> superclass = compared.getSuperclass();
			while(null != superclass){
				deep++;
				if(superclass.equals(obj))
					return deep;
				superclass = superclass.getSuperclass();
			}
		}
		
		if(!compared.isInterface()){
			deep = 0;
			Class<?>[] is = compared.getInterfaces();
			for(Class<?> c : is){
				if(c.equals(obj)){
					deep++;
					return deep;
				}
			}
			Class<?> superclass = compared.getSuperclass();
			while(null != superclass){
				deep++;
				if(superclass.equals(obj)){
					return deep;
				}
				is = superclass.getInterfaces();
				for(Class<?> c : is){
					if(c.equals(obj)){
						deep++;
						return deep;
					}
					Class<?> superclass1 = c.getSuperclass();
					while(null != superclass1){
						deep++;
						if(superclass1.equals(obj)){
							return deep;
						}
						superclass1 = superclass1.getSuperclass();
					}
				}
				superclass = superclass.getSuperclass();
			}
		}
		return 0;
	}
	
	public static Field getField(Class<?> clazz, String fieldName){
		Field[] fs = clazz.getDeclaredFields();
		for(Field f : fs){
			if(f.getName().equals(fieldName))
				return f;
		}
		return null;
	}
	/**
	 * 获得字段的get方法名称
	 * @param fieldName
	 * @return 
	 */
	public static String getGetterMethodName(String fieldName){
		return "get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
	}
	/**
	 * 获得boolean类型字段的get方法名称
	 * @param fieldName
	 * @return 
	 */
	public static String getGetterMethodName(String fieldName, boolean isbool){
		if(isbool)
			return "is" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
		else
			return getGetterMethodName(fieldName);
	}
	/**
	 * 获得字段的set方法名称
	 * @param fieldName
	 * @return 
	 */
	public static String getSetterMethodName(String fieldName){
		return "set" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
	}
	/**
	 * 获得指定类中某个get方法
	 * @param clazz
	 * @param methodName
	 * @return
	 */
	public static Method getGetterMethod(Class<?> clazz, String methodName) throws Exception {
		return clazz.getMethod(methodName);
	}
	/**
	 * 获得指定类中某个get方法
	 * @param clazz
	 * @param methodName
	 * @return
	 */
	public static Method getSetterMethod(Class<?> clazz, String methodName, Class<?>... paratypes) throws Exception {
		return clazz.getMethod(methodName, paratypes);
	}
	/**
	 * 字段是否有get和set方法
	 * @param clazz
	 * @param field
	 * @return
	 * @throws Exception
	 */
	public static boolean hasGetterAndSetter(Class<?> clazz, Field field) throws Exception {
		boolean isbool = field.getType() == Boolean.class || field.getType() == boolean.class;
		Method gm = null;
		Method sm = null;
		try {
			gm = getGetterMethod(clazz, getGetterMethodName(field.getName(), isbool));
			sm = getSetterMethod(clazz, getSetterMethodName(field.getName()), field.getType());
		} catch (NoSuchMethodException e) {
			return false;
		}
		return (gm != null && sm != null);
	}
	
}
