My Tomcat JDBC Extension

 I had an issue with a long running SQL in spring batch. The problem
 was ultimately that the extract would run, and for each rs.next() it
 would do formatting and output a 'line' of SQL.  This probably needs to
 be refactored, but it lead me to look at the ResetAbandonedTimer
 that comes with tomcat-jdbc.  It resets the timer used by the 
 removeAbandoned='true' process on the datasource. However, it only resets
 the timer on calls to java.sql.Statement (or subclass thereof).  I wrote
 this interceptor to extend that ResetAbandonedTimer class and reset the
 timer on calls to next().
 