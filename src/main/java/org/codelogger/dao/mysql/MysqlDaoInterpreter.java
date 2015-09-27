package org.codelogger.dao.mysql;

import static com.google.common.collect.Lists.newArrayList;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import org.codelogger.dao.MysqlDao;
import org.codelogger.dao.exception.DataAccessException;
import org.codelogger.dao.exception.MysqlSqlException;
import org.codelogger.dao.stereotype.Entity;

public class MysqlDaoInterpreter<E, I extends Serializable> implements MysqlDao<E, I> {

  @SuppressWarnings("unchecked")
  public MysqlDaoInterpreter(final Properties settings, final Class<?> daoClass) {

    dataSourcePool = new DataSourcePool(settings);
    Type genericInterface = daoClass.getGenericInterfaces()[0];
    String[] typeNames = genericInterface.getTypeName().split(",");
    try {
      entityClass = (Class<E>) Class.forName(typeNames[0].substring(typeNames[0].indexOf("<") + 1));
      tableName = entityClass.getAnnotation(Entity.class).tableName();
      entityFields = entityClass.getDeclaredFields();
      idClass = (Class<I>) Class.forName(typeNames[1].substring(0, typeNames[1].length() - 1)
        .trim());
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  @Override
  public E findOne(final I id) {

    return null;
  }

  @Override
  public List<E> findAll() {

    List<E> elements = newArrayList();
    Connection connection = dataSourcePool.getConnection();
    try {
      Statement statement = connection.createStatement();
      ResultSet resultSet = statement.executeQuery("select * from " + tableName);
      try {
        while (resultSet.next()) {
          E element = entityClass.newInstance();
          for (Field field : entityFields) {
            if (Modifier.isFinal(field.getModifiers())) {
              continue;
            }
            field.setAccessible(true);
            Object value = null;
            Type genericType = field.getGenericType();
            String fieldName = field.getName();
            if (genericType == Integer.class || genericType == int.class) {
              value = resultSet.getInt(fieldName);
            } else if (genericType == Long.class || genericType == long.class) {
              value = resultSet.getLong(fieldName);
            } else if (genericType == String.class) {
              value = resultSet.getString(fieldName);
            } else if (genericType == Boolean.class || genericType == boolean.class) {
              value = resultSet.getBoolean(fieldName);
            } else if (genericType == Byte.class || genericType == byte.class) {
              value = resultSet.getByte(fieldName);
            } else if (genericType == Short.class || genericType == short.class) {
              value = resultSet.getShort(fieldName);
            }
            if (value != null) {
              field.set(element, value);
            }
          }
          elements.add(element);
        }
        resultSet.close();
      } catch (Exception e) {
        throw new DataAccessException(e);
      }
      statement.close();
    } catch (SQLException e) {
      throw new MysqlSqlException(e);
    }
    dataSourcePool.freeConnection(connection);
    return elements;
  }

  @Override
  public E save(final E e) {

    return null;
  }

  @Override
  public void delete(final E e) {

  }

  @Override
  public void delete(final I id) {

  }

  @Override
  public Long count() {

    Long count = 0L;
    Connection connection = dataSourcePool.getConnection();
    try {
      Statement statement = connection.createStatement();
      ResultSet resultSet = statement.executeQuery("select count(*) as ecount from " + tableName);
      count = resultSet.next() ? resultSet.getLong("ecount") : 0;
    } catch (SQLException e) {
      throw new MysqlSqlException(e);
    }
    dataSourcePool.freeConnection(connection);
    return count;
  }

  private String tableName;

  private Field[] entityFields;

  private Class<E> entityClass;

  private Class<I> idClass;

  private DataSourcePool dataSourcePool;
}
