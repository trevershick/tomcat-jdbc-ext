package com.tshick.tomcat.jdbc.pool.interceptor;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.logging.Logger;

import org.apache.tomcat.jdbc.pool.interceptor.ResetAbandonedTimer;

public class ResultSetResetAbandonedTimer extends ResetAbandonedTimer {
	private final Logger log = Logger.getLogger(getClass().getName());
	
	private static final String PREPARE_STATEMENT = "prepareStatement";
	private static final String CREATE_STATEMENT = "createStatement";
	private static final Class<?>[] STATEMENT_CLASS = new Class[] { Statement.class };
	private static final Class<?>[] PREPAREDSTMT_CLASS = new Class[] { PreparedStatement.class };
	private static final Class<?>[] RESULTSET_CLASS = new Class[]{ResultSet.class};
	private static final ClassLoader CLASS_LOADER = ResultSetResetAbandonedTimer.class.getClassLoader();

	@Override
	public Object createStatement(Object proxy, Method method, Object[] args,
			Object statement, long time) {
		final String methodName = method.getName();
		
		if (methodName.equals(PREPARE_STATEMENT)) {
			Object returnedStatent = super.createStatement(proxy, method, args, statement, time);
			return Proxy.newProxyInstance(CLASS_LOADER,PREPAREDSTMT_CLASS, new ResultSetProxyInjector(returnedStatent));					
		} else if (methodName.equals(CREATE_STATEMENT)) {
			Object returnedStatent = super.createStatement(proxy, method, args, statement, time);
			return Proxy.newProxyInstance(CLASS_LOADER,STATEMENT_CLASS, new ResultSetProxyInjector(returnedStatent));					
		} else {
			return super.createStatement(proxy, method, args, statement, time);
		}
	}
	
	/**
	 * Proxy InvocationHandler for java.sql.Statement(s), this creates a proxy
	 * around any ResultSet returned from a statement method. 
	 * @see TimerResettingResult
	 */
	class ResultSetProxyInjector implements InvocationHandler {
		
		private Object delegate;
		
		public ResultSetProxyInjector(Object delegate) {
			this.delegate = delegate;
		}
		
		public Object invoke(Object proxy, Method method, Object[] args)
				throws Throwable {
			final Object o = method.invoke(delegate, args);
			if (o instanceof ResultSet) {
				Object proxiedResultSet = Proxy.newProxyInstance(CLASS_LOADER, RESULTSET_CLASS,
						new TimerResettingResult((ResultSet) o));
				return proxiedResultSet;
			}
			return o;
		}
		@Override
		protected void finalize() throws Throwable {
			log.fine("ResultSetProxyInjector.finalize()");
			delegate = null;
			super.finalize();
		}
	}
	/**
	 * Resets the abandoned timer on calls to 'next'
	 */
	class TimerResettingResult implements InvocationHandler {
		private static final String NEXT = "next";
		
		private ResultSet delegate;
		
		public TimerResettingResult(ResultSet rs) {
			this.delegate = rs;
		}

		public Object invoke(Object obj, Method method, Object[] arguments)
				throws Throwable {
			if (NEXT.equals(method.getName())) {
				resetTimer();
			}
			return method.invoke(delegate, arguments);
		}
		@Override
		protected void finalize() throws Throwable {
			log.fine("TimerResettingResult.finalize()");
			delegate = null;
			super.finalize();
		}
	}

}
