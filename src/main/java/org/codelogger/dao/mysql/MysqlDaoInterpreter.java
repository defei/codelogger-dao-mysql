package org.codelogger.dao.mysql;

import static com.google.common.collect.Lists.newArrayList;
import static java.lang.String.format;
import static org.codelogger.utils.CollectionUtils.join;
import static org.codelogger.utils.StringUtils.isBlank;
import static org.codelogger.utils.StringUtils.isNotBlank;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.codelogger.dao.MysqlDao;
import org.codelogger.dao.exception.DataAccessException;
import org.codelogger.dao.exception.MysqlSqlException;
import org.codelogger.dao.stereotype.Column;
import org.codelogger.dao.stereotype.Entity;
import org.codelogger.dao.stereotype.Id;
import org.codelogger.utils.ArrayUtils;

public class MysqlDaoInterpreter<E, I extends Serializable> implements MysqlDao<E, I> {

  @SuppressWarnings("unchecked")
  public MysqlDaoInterpreter(final Properties settings, final Class<?> daoClass) {

    dataSourcePool = new DataSourcePool(settings);
    Type genericInterface = daoClass.getGenericInterfaces()[0];
    String[] typeNames = genericInterface.getTypeName().split(",");
    try {
      entityClass = (Class<E>) Class.forName(typeNames[0].substring(typeNames[0].indexOf("<") + 1));
      tableName = entityClass.getAnnotation(Entity.class).tableName();
      Field[] declaredFields = entityClass.getDeclaredFields();
      List<String> columnNames = newArrayList();
      List<String> columnValuePlaceholders = newArrayList();
      List<String> columnUpdateNameValuePair = newArrayList();
      if (ArrayUtils.isNotEmpty(declaredFields)) {
        entityFields = newArrayList(declaredFields);
        Iterator<Field> iterator = entityFields.iterator();
        while (iterator.hasNext()) {
          Field field = iterator.next();
          if (Modifier.isFinal(field.getModifiers()) || Modifier.isTransient(field.getModifiers())
            || Modifier.isStatic(field.getModifiers())) {
            iterator.remove();
          }
          field.setAccessible(true);
          Id id = field.getAnnotation(Id.class);
          Column column = field.getAnnotation(Column.class);
          String fieldName = field.getName();
          String columnName = fieldName;
          if (id != null) {
            idName = isBlank(id.name()) ? fieldName : id.name();
            idField = field;
            columnName = idName;
          } else if (column != null && isNotBlank(column.name())) {
            columnName = column.name();
          }
          columnNames.add(columnName);
          columnValuePlaceholders.add(":" + fieldName);
          columnUpdateNameValuePair.add(columnName + " = :" + fieldName);
        }
      }
      insertSql = format("insert into %s (%s) values (%s)", tableName, join(columnNames, ","),
        join(columnValuePlaceholders, ","));
      updateSql = format("update %s set %s where %s = :%s", tableName,
        join(columnUpdateNameValuePair, ","), idName, idField.getName());
      idClass = (Class<I>) Class.forName(typeNames[1].substring(0, typeNames[1].length() - 1)
        .trim());
      countSql = format("select count(%s) from %s", idName, tableName);
      deleteSql = format("delete from %s where %s = %%s", tableName, idName);
      lockTableSql = format("lock table %s write", tableName);
      unlockTableSql = "unlock tables";
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  @Override
  public E findOne(final I id) {

    E result = null;
    Connection connection = dataSourcePool.getConnection();
    try {
      Statement statement = connection.createStatement();
      String sql = "select * from " + tableName + " where " + idName + " = " + coverToSqlValue(id);
      ResultSet resultSet = statement.executeQuery(sql);
      try {
        while (resultSet.next()) {
          result = entityClass.newInstance();
          for (Field field : entityFields) {
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
              field.set(result, value);
            }
          }
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
    return result;
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
  public E save(final E entity) {

    Object id = getFieldValue(idField, entity);
    String sql;
    Connection connection = dataSourcePool.getConnection();
    try {
      Statement statement = connection.createStatement();
      if (id == null) {
        sql = insertSql;
        lockTable(statement);
        ResultSet resultSet = statement.executeQuery(format(
          "select %s from %s order by %s desc limit 1", idName, tableName, idName));
        Long nextId = resultSet.next() ? resultSet.getLong(1) + 1 : 0;
        sql = sql.replaceAll(":" + idField.getName(), coverToSqlValue(nextId));
        for (Field field : entityFields) {
          sql = sql
            .replaceAll(":" + field.getName(), coverToSqlValue(getFieldValue(field, entity)));
        }
        statement.execute(sql);
        unlockTable(statement);
        setFieldValue(entity, idField, nextId);
      } else {
        sql = updateSql;
        for (Field field : entityFields) {
          sql = sql
            .replaceAll(":" + field.getName(), coverToSqlValue(getFieldValue(field, entity)));
        }
        statement.execute(sql);
      }
      statement.close();
    } catch (SQLException e) {
      throw new MysqlSqlException(e);
    }
    dataSourcePool.freeConnection(connection);
    return entity;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void delete(final E e) {

    for (Field field : entityFields) {
      Id id = field.getAnnotation(Id.class);
      if (id != null) {
        try {
          delete((I) field.get(e));
        } catch (Exception e1) {
          e1.printStackTrace();
        }
      }
    }
  }

  @Override
  public void delete(final I id) {

    Connection connection = dataSourcePool.getConnection();
    try {
      Statement statement = connection.createStatement();
      statement.execute(format(deleteSql, coverToSqlValue(id)));
      statement.close();
    } catch (SQLException e) {
      throw new MysqlSqlException(e);
    }
    dataSourcePool.freeConnection(connection);
  }

  @Override
  public Long count() {

    Long count = 0L;
    Connection connection = dataSourcePool.getConnection();
    try {
      Statement statement = connection.createStatement();
      ResultSet resultSet = statement.executeQuery(countSql);
      count = resultSet.next() ? resultSet.getLong(1) : 0;
    } catch (SQLException e) {
      throw new MysqlSqlException(e);
    }
    dataSourcePool.freeConnection(connection);
    return count;
  }

  private void lockTable(final Statement statement) throws SQLException {

    statement.execute(lockTableSql);
  }

  private void unlockTable(final Statement statement) throws SQLException {

    statement.execute(unlockTableSql);
  }

  private String coverToSqlValue(final Object value) {

    if (value == null) {
      return "null";
    } else if (value instanceof String) {
      return format("'%s'", value);
    } else {
      return value.toString();
    }
  }

  private Object getFieldValue(final Field field, final Object source) {

    try {
      return field.get(source);
    } catch (Exception e) {
      throw new DataAccessException(e);
    }
  }

  private void setFieldValue(final Object target, final Field filed, final Object value) {

    try {
      filed.set(target, value);
    } catch (Exception e) {
      throw new DataAccessException(e);
    }
  }

  private String deleteSql;

  private String updateSql;

  private String insertSql;

  private String countSql;

  private String lockTableSql;

  private String unlockTableSql;

  private String idName;

  private String tableName;

  private List<Field> entityFields;

  private Field idField;

  private Class<E> entityClass;

  private Class<I> idClass;

  private DataSourcePool dataSourcePool;
}
