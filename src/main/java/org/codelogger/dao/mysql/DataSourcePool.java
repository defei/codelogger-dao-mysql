package org.codelogger.dao.mysql;

import java.sql.Connection;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.codelogger.dao.exception.MysqlConnectionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mysql.jdbc.Driver;
import com.mysql.jdbc.NonRegisteringDriver;

public class DataSourcePool {

  private static final Logger logger = LoggerFactory.getLogger(DataSourcePool.class);

  public DataSourcePool(final Properties settings) {

    if (settings == null) {
      throw new IllegalArgumentException("Datasource settings can not be null.");
    }
    throwIllegalArgumentExceptionIfNull(settings, HOST_PROPERTY_KEY);
    throwIllegalArgumentExceptionIfNull(settings, DBNAME_PROPERTY_KEY);
    throwIllegalArgumentExceptionIfNull(settings, USER_PROPERTY_KEY);
    throwIllegalArgumentExceptionIfNull(settings, PASSWORD_PROPERTY_KEY);

    this.settings.setProperty(HOST_PROPERTY_KEY, settings.getProperty(HOST_PROPERTY_KEY));
    this.settings.setProperty(NonRegisteringDriver.DBNAME_PROPERTY_KEY,
      settings.getProperty(DBNAME_PROPERTY_KEY));
    this.settings.setProperty(NonRegisteringDriver.PORT_PROPERTY_KEY,
      settings.getProperty(PORT_PROPERTY_KEY, "3306"));
    this.settings.setProperty(NonRegisteringDriver.USER_PROPERTY_KEY,
      settings.getProperty(USER_PROPERTY_KEY));
    this.settings.setProperty(NonRegisteringDriver.PASSWORD_PROPERTY_KEY,
      settings.getProperty(PASSWORD_PROPERTY_KEY, ""));
  }

  public Connection getConnection() {

    try {
      Connection connection = connections.poll();
      if (connection == null) {
        logger.debug("Thers no more connection in poll, get a new one.");
        Class<?> driverClass = Class.forName(JDBC_DRIVER);
        Driver driver = (Driver) driverClass.newInstance();
        String mysqlHost = settings.getProperty(HOST_PROPERTY_KEY);
        return driver.connect(mysqlHost.startsWith(JDBC_MYSQL_HOST_PREFIX) ? mysqlHost
          : JDBC_MYSQL_HOST_PREFIX + mysqlHost, settings);
      } else {
        return connection;
      }
    } catch (Exception e) {
      throw new MysqlConnectionException(e);
    }
  }

  public void freeConnection(final Connection connection) {

    connections.add(connection);
  }

  private void throwIllegalArgumentExceptionIfNull(final Properties settings,
    final String configPeopertyKey) {

    if (settings.getProperty(HOST_PROPERTY_KEY) == null) {
      throw new IllegalArgumentException(String.format("Argument %s can not be null.",
        configPeopertyKey));
    }
  }

  private Queue<Connection> connections = new ConcurrentLinkedDeque<Connection>();

  private Properties settings = new Properties();

  private static final String JDBC_MYSQL_HOST_PREFIX = "jdbc:mysql://";

  private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";

  private static final String PASSWORD_PROPERTY_KEY = "database.password";

  private static final String USER_PROPERTY_KEY = "database.user";

  private static final String PORT_PROPERTY_KEY = "database.port";

  private static final String DBNAME_PROPERTY_KEY = "database.name";

  private static final String HOST_PROPERTY_KEY = "database.host";
}
