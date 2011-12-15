package com.tshick.tomcat.jdbc.pool.interceptor;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.tomcat.jdbc.pool.DataSourceFactory;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.base.Throwables;

public class ResultSetResetAbandonedTimerTest {

	private static DataSource  dataSourceWithResetter;
	private static DataSource  dataSourceWithoutResetter;

	private static final int recordCount = 100;
	private static final int abandonedTimeoutInSeconds = 3;
	
	
	private static final Function<ResultSet,Void> CALLBACKWITHDELAY = new Function<ResultSet, Void>() {
		final long delayInMs = (abandonedTimeoutInSeconds * 1000) / (recordCount/2);
		public Void apply(ResultSet input) {
			try {
				input.getString(1);
				Thread.sleep(delayInMs);
			} catch (Exception e) {
				Throwables.propagate(e);
			}
			return null;
		}
	};
	
	
	@BeforeClass
	public static void setup() throws Exception {
		dataSourceWithResetter = createDatasource(true);
		dataSourceWithoutResetter = createDatasource(false);
		
		populateDatabase(dataSourceWithResetter);
		populateDatabase(dataSourceWithoutResetter);
		
	}

	protected static void populateDatabase(DataSource ds) {
		execute(ds,"CREATE TABLE TEST_TABLE ( xxx VARCHAR(32) NOT NULL PRIMARY KEY)");
		for (int i=0;i < recordCount;i++) {
			execute(ds, String.format("INSERT INTO TEST_TABLE VALUES ('%s')", "value" + i));
		}
	}

	private static void execute(DataSource ds, String string) {
		try {
			Connection c = ds.getConnection();
			Statement stmt = c.createStatement();
			stmt.execute(string);
			stmt.close();
			c.close();
		} catch (SQLException sqle) {
			Throwables.propagate(sqle);
		}
	}
	
	private static void query(DataSource ds, Function<ResultSet,Void> callback, String string) {
		try {
			Connection c = ds.getConnection();
			Statement stmt = c.createStatement();
			ResultSet rs = stmt.executeQuery(string);
			while (rs.next()) {
				callback.apply(rs);
			}
			rs.close();
			stmt.close();
			c.close();
		} catch (SQLException sqle) {
			Throwables.propagate(sqle);
		}
	}

	protected static DataSource  createDatasource(boolean addInterceptor) throws Exception {
		Properties p = new Properties();
		p.setProperty("driverClassName", "org.h2.Driver");
		p.setProperty("initialSize", "5");
		p.setProperty("jmxEnabled", "false");
		if (addInterceptor) {
			p.setProperty("jdbcInterceptors","com.tshick.tomcat.jdbc.pool.interceptor.ResultSetResetAbandonedTimer");
		}
		p.setProperty("logAbandoned", "true");
		p.setProperty("maxActive", "20");
		p.setProperty("maxIdle", "10");
		p.setProperty("minIdle", "3");
		p.setProperty("name", "database"+ (addInterceptor ? "intercepted":"unintercepted"));
		p.setProperty("removeAbandoned", "true");
		p.setProperty("removeAbandonedTimeout", String.valueOf(abandonedTimeoutInSeconds));
		p.setProperty("type", "javax.sql.DataSource");
		p.setProperty("url", "jdbc:h2:mem:" + (addInterceptor ? "intercepted":"unintercepted"));
		p.setProperty("password", "");
		p.setProperty("username", "sa");
		return new DataSourceFactory().createDataSource(p);
	}
	
	@Test
	public void negative_test() throws SQLException {
		// wait 'delayInMs' to force the error
		try {
			query(dataSourceWithoutResetter, CALLBACKWITHDELAY, "SELECT * FROM TEST_TABLE");
			fail("Should have thrown a SQL Exception");
		} catch (RuntimeException re) {
			assertTrue(Throwables.getRootCause(re).getMessage().contains("object is already closed"));
		}
	}
	
	@Test
	public void positive_test() {
		query(dataSourceWithResetter, CALLBACKWITHDELAY, "SELECT * FROM TEST_TABLE");
	}
}
