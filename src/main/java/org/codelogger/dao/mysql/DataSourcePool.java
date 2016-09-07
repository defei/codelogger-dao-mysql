package org.codelogger.dao.mysql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;

import org.codelogger.dao.exception.MysqlConnectionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mysql.jdbc.Driver;
import com.mysql.jdbc.NonRegisteringDriver;

public class DataSourcePool {

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
    maxConnectionSizeInPool = Integer.valueOf(settings.getProperty(MAX_CONNECTION_SIZE_IN_POOL,
      "10"));
    minConnectionSizeInPool = Integer.valueOf(settings
      .getProperty(MIN_CONNECTION_SIZE_IN_POOL, "1"));
    maxIdleTime = Integer.valueOf(settings.getProperty(CONNECTION_MAX_IDLE_TIME, "1800000"));

    Timer timer = new Timer();
    timer.schedule(new TimerTask() {

      @Override
      public void run() {

        int totalConnection = connections.size();
        logger.trace("Test total[{}] connections if enable.", totalConnection);
        for (int i = 0; i < totalConnection; i++) {
          Connection connection = connections.poll();
          if (connection != null) {
            try {
              Statement statement = connection.createStatement();
              statement.execute("SELECT 1");
              statement.close();
              connections.add(connection);
            } catch (SQLException e) {
              logger.info("Connect is disabled.", e);
            }
          }
        }
      }
    }, 0, TimeUnit.SECONDS.toMillis(30));

    dataSourcePools.put(this, System.currentTimeMillis());
  }

  /**
   * 获取数据库连接
   */
  public Connection getConnection() {

    try {
      Connection connection = connections.poll();
      if (connection == null) {
        logger.debug("There no more connection in poll, get a new one.");
        Class<?> driverClass = Class.forName(JDBC_DRIVER);
        Driver driver = (Driver) driverClass.newInstance();
        String mysqlHost = settings.getProperty(HOST_PROPERTY_KEY);
        connection = driver.connect(mysqlHost.startsWith(JDBC_MYSQL_HOST_PREFIX) ? mysqlHost
          : JDBC_MYSQL_HOST_PREFIX + mysqlHost, settings);
        connectionPool.put(connection, System.currentTimeMillis());
      }
      return connection;
    } catch (Exception e) {
      throw new MysqlConnectionException(e);
    }
  }

  /**
   * 释放连接，如果池有空间则放回连接池，如果没有则直接关闭。
   */
  public void freeConnection(final Connection connection) {

    if (connections.size() < maxConnectionSizeInPool) {
      connections.add(connection);
      connectionPool.put(connection, System.currentTimeMillis());
    } else {
      try {
        connection.close();
      } catch (SQLException e) {
        logger.debug("Close connection failed.", e);
      }
    }
  }

  /**
   * 关闭所有数据库连接
   */
  public static void closeAllConnections() {

    logger.info("Release all connections.");

    for (Map.Entry<DataSourcePool, Long> connectionAndConstructTime : dataSourcePools.entrySet()) {
      DataSourcePool dataSourcePool = connectionAndConstructTime.getKey();
      for (Connection connection : dataSourcePool.connections) {
        try {
          connection.close();
        } catch (SQLException e) {
          logger.info("Close connection[{}] failed.", connection, e);
        }
      }
    }

  }

  private void throwIllegalArgumentExceptionIfNull(final Properties settings,
    final String configPropertyKey) {

    if (settings.getProperty(HOST_PROPERTY_KEY) == null) {
      throw new IllegalArgumentException(String.format("Argument %s can not be null.",
        configPropertyKey));
    }
  }

  private final ConcurrentHashMap<Connection, Long> connectionPool = new ConcurrentHashMap<Connection, Long>();

  private static final ConcurrentHashMap<DataSourcePool, Long> dataSourcePools = new ConcurrentHashMap<DataSourcePool, Long>();

  private Queue<Connection> connections = new ConcurrentLinkedDeque<Connection>();

  private Properties settings = new Properties();

  private int maxIdleTime;

  private int minConnectionSizeInPool;

  private int maxConnectionSizeInPool;

  static {
    Timer timer = new Timer();
    timer.schedule(new TimerTask() {

      @Override
      public void run() {

        logger.trace("Check total[{}] DataSourcePool connections idle status .",
          dataSourcePools.size());

        for (Map.Entry<DataSourcePool, Long> connectionAndConstructTime : dataSourcePools
          .entrySet()) {
          DataSourcePool dataSourcePool = connectionAndConstructTime.getKey();
          for (Map.Entry<Connection, Long> dataSourcePoolAndLastUsedTime : dataSourcePool.connectionPool
            .entrySet()) {
            Connection connection = dataSourcePoolAndLastUsedTime.getKey();
            long idleTime = System.currentTimeMillis() - dataSourcePoolAndLastUsedTime.getValue();
            if (idleTime > dataSourcePool.maxIdleTime
              && dataSourcePool.connections.size() > dataSourcePool.minConnectionSizeInPool) {
              logger.debug("Connection[{}] idle too much, release from datasourcePool[{}].",
                connection, dataSourcePool);
              dataSourcePool.connections.remove(connection);
            }
          }
        }
      }
    }, 0, TimeUnit.MINUTES.toMillis(5));
  }

  private static final String JDBC_MYSQL_HOST_PREFIX = "jdbc:mysql://";

  private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";

  private static final String PASSWORD_PROPERTY_KEY = "database.password";

  private static final String USER_PROPERTY_KEY = "database.user";

  private static final String PORT_PROPERTY_KEY = "database.port";

  private static final String DBNAME_PROPERTY_KEY = "database.name";

  private static final String HOST_PROPERTY_KEY = "database.host";

  private static final String CONNECTION_MAX_IDLE_TIME = "database.connection.max.idle.time";

  private static final String MIN_CONNECTION_SIZE_IN_POOL = "database.min.connection.in.pool";

  private static final String MAX_CONNECTION_SIZE_IN_POOL = "database.max.connection.in.pool";

  private static final Logger logger = LoggerFactory.getLogger(DataSourcePool.class);
}
