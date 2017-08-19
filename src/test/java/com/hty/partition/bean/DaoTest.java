package com.hty.partition.bean;

import java.sql.SQLException;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;

import com.hty.partition.dao.BaseDao;
import com.hty.partition.dao.impl.HibernateBaseDaoImpl;

public class DaoTest {
	
	public static void main(String[] args) throws Exception {
		SessionFactory sf = getSessionFactory();
		BaseDao dao = new HibernateBaseDaoImpl(sf);
		Session s = sf.openSession();
		s.beginTransaction();
		String sql = "select 1 from dual";
		dao.executeSQLQuery(sql);
		sf.close();
	}
	
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
}
