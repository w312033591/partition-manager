package com.hty.partition.bean;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.junit.Test;

import com.hty.partition.dao.BaseDao;
import com.hty.partition.dao.PartitionDaoFactory;

public class ProxyDaoTest {
	
	private static SessionFactory getSessionFactory() {
		final StandardServiceRegistry registry = new StandardServiceRegistryBuilder()
				.configure() // configures settings from hibernate.cfg.xml
				.build();
		try {
			SessionFactory sessionFactory = new MetadataSources(registry).buildMetadata()
					.buildSessionFactory();
			return sessionFactory;
		} catch (Exception e) {
			StandardServiceRegistryBuilder.destroy(registry);
		}
		return null;
	}
	
	
	private int fixStart(int len, int pos) {
		return pos < 0 ? 0 : (pos > len ? len : pos);
	}
	Set<Integer> skippedParameter = new HashSet<Integer>();
	List<Integer> placeholderPos = new ArrayList<Integer>();
	@Test
	public void testCreateProxy(){
		//BaseDao dao = PartitionDaoFactory.createProxyDao();
		String pattern_one = "\\$one\\(([^)]+),(\\s)*\\?\\)";
		String pattern_list = "\\$list\\(([^)]+),(\\s)*\\?\\)";
		String sql = "select * from $one(t_order,?) a where a.id=?  $one(t_order,\t \n?) ";
		String sql1 = "select * from $list(t_order, ?) a where a.userid=?";
		Pattern p = Pattern.compile("\\?");
		Matcher m = p.matcher(sql);
		while(m.find()) {
			placeholderPos.add(m.start());
			System.out.println(m.start());
		}
		
		Pattern  p1 = Pattern.compile(pattern_one);
		Matcher m1 = p1.matcher(sql);
		while(m1.find()){
			System.out.println(m1.group(1).trim());
			System.out.println(m1.start());
			System.out.println(m1.end());
			addArgsFilter(m1.start(), m1.end());
		}
		System.out.println(skippedParameter);
		System.out.println(sql.replaceAll(pattern_one, "$1"));
	}
	
	private void addArgsFilter(int start, int end) {
		for(int i = 0; i < placeholderPos.size(); i++) {
			Integer q = placeholderPos.get(i);
			if(q >= start && q < end) {
				skippedParameter.add(i);
			}
		}
	}
	
	
	@Test
	public void test1() throws Exception {
		SessionFactory sf = getSessionFactory();
		BaseDao dao = PartitionDaoFactory.createProxyDao(sf);
		Session s = sf.openSession();
		s.beginTransaction();
		String orderid = "2017081305020000051";
		String sql = "select * from $one(t_order, ?) a where a.id = ?";
		try {
			dao.executeSQLQuery(sql, orderid, orderid);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		sf.close();
	}
	
	@Test
	public void test11() throws Exception {
		
	}
	
	
	
	
	
}
