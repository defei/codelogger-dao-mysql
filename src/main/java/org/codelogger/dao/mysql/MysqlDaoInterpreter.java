package org.codelogger.dao.mysql;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static java.lang.String.format;
import static org.codelogger.utils.ArrayUtils.isArray;
import static org.codelogger.utils.CollectionUtils.isCollection;
import static org.codelogger.utils.CollectionUtils.join;
import static org.codelogger.utils.StringUtils.isBlank;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.codelogger.core.bean.Page;
import org.codelogger.core.bean.Pageable;
import org.codelogger.dao.MysqlDao;
import org.codelogger.dao.exception.DataAccessException;
import org.codelogger.dao.exception.MethodUnsupportException;
import org.codelogger.dao.exception.MysqlSqlException;
import org.codelogger.dao.stereotype.Column;
import org.codelogger.dao.stereotype.Entity;
import org.codelogger.dao.stereotype.Id;
import org.codelogger.dao.stereotype.Param;
import org.codelogger.dao.stereotype.Query;
import org.codelogger.utils.ArrayUtils;
import org.codelogger.utils.CollectionUtils;
import org.codelogger.utils.StringUtils;

public class MysqlDaoInterpreter<E, I extends Serializable> implements MysqlDao<E, I> {

  @SuppressWarnings("unchecked")
  public MysqlDaoInterpreter(final Properties settings, final Class<?> daoClass) {

    dataSourcePool = new DataSourcePool(settings);

    Type genericInterface = daoClass.getGenericInterfaces()[0];
    String[] typeNames = genericInterface.toString().split(",");
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
          fieldNameToField.put(field.getName(), field);
          fieldNameToField.put(StringUtils.firstCharToUpperCase(field.getName()), field);
          if (Modifier.isFinal(field.getModifiers()) || Modifier.isTransient(field.getModifiers())
            || Modifier.isStatic(field.getModifiers())) {
            iterator.remove();
          }
          field.setAccessible(true);
          Id id = field.getAnnotation(Id.class);
          String fieldName = field.getName();
          String columnName = getFieldColumnName(field);
          if (id != null) {
            idName = isBlank(id.name()) ? columnName : id.name();
            idField = field;
            columnName = idName;
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
      Method[] declaredMethods = daoClass.getDeclaredMethods();
      if (ArrayUtils.isNotEmpty(declaredMethods)) {
        for (Method method : declaredMethods) {
          System.out.println(method.getName());
          methodNameToMethod.put(method.getName(), method);
        }
      }
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  @Override
  public E findOne(final I id) {

    String sql = "select * from " + tableName + " where " + idName + " = " + coverToSqlValue(id);
    return findOne(sql);
  }

  @Override
  public List<E> findAll() {

    String sql = "select * from " + tableName;
    return findAll(sql);
  }

  @Override
  public Page<E> findAll(final Pageable pageable) {

    String sql = "select * from " + tableName + buildLimitSql(pageable);
    List<E> content = findAll(sql);
    Long count = count();
    return new Page<E>(pageable.page, pageable.pageSize, count, (int) (count / pageable.pageSize),
      content);
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

    return count(countSql);
  }

  @SuppressWarnings("unchecked")
  public Object executeByMethod(final Method method, final Object... args) {

    if (method.equals(findOneMethod)) {
      return findOne((I) args[0]);
    }
    if (method.equals(saveMethod)) {
      return save((E) args[0]);
    }
    if (method.equals(findAllMethod)) {
      return findAll();
    }
    if (method.equals(countMethod)) {
      return count();
    }
    if (method.equals(deleteMethod)) {
      delete((I) args[0]);
      return null;
    }
    if (method.equals(pageableFindAllMethod)) {
      return findAll((Pageable) args[0]);
    }

    Query query = method.getAnnotation(Query.class);
    if (query == null) {
      if (method.getName().startsWith("findBy")) {
        StringBuilder sqlBuilder = new StringBuilder("select * from " + tableName + " where ");
        String conditions = method.getName().replaceFirst("findBy", "");
        String[] fieldNames = conditions.split("And");
        for (int i = 0; i < fieldNames.length; i++) {
          Field field = fieldNameToField.get(fieldNames[i]);
          sqlBuilder.append(getFieldColumnName(field)).append("=");
          sqlBuilder.append(coverToSqlValue(args[i]));
        }
        String sql = sqlBuilder.toString();
        if (Collection.class.isAssignableFrom(method.getReturnType())) {
          return findAll(sql);
        } else if (Page.class.isAssignableFrom(method.getReturnType())) {
          Object lastArgument = ArrayUtils.getLastElement(args);
          Pageable pageable = null;
          Long count;
          if (lastArgument instanceof Pageable) {
            count = count(sql.replace("select *", "select count(*)"));
            pageable = (Pageable) lastArgument;
            sql += buildLimitSql(pageable);
          } else {
            throw new IllegalArgumentException("Pageable is not the latest argument.");
          }
          List<E> content = findAll(sql);
          return new Page<E>(pageable.page, pageable.pageSize, count,
            (int) (count / pageable.pageSize), content);
        } else {
          return findOne(sql);
        }
      }
      throw new MethodUnsupportException(method.toString());
    } else {
      String querySql = query.value().trim();
      Annotation[][] parameterAnnotations = method.getParameterAnnotations();
      for (int i = 0; i < parameterAnnotations.length; i++) {
        Annotation[] annotations = parameterAnnotations[i];
        Param param = null;
        for (Annotation annotation : annotations) {
          if (annotation instanceof Param) {
            param = (Param) annotation;
            break;
          }
        }
        querySql = querySql.replaceFirst(":" + param.value(), coverToSqlValue(args[i]));
      }
      querySql = querySql.replaceFirst(entityClass.getName(), tableName);
      querySql = querySql.replaceFirst(
        ArrayUtils.getLastElement(entityClass.getName().split("\\.")), tableName);
      if (isSelectQuery(querySql)) {
        querySql = simpleSelectQueryPattern.matcher(querySql).matches() ? "select * " + querySql
          : querySql;
        if (Collection.class.isAssignableFrom(method.getReturnType())) {
          return findAll(querySql);
        } else if (Page.class.isAssignableFrom(method.getReturnType())) {
          Object lastArgument = ArrayUtils.getLastElement(args);
          Pageable pageable = null;
          Long count;
          if (lastArgument instanceof Pageable) {
            count = count(querySql.replace("select *", "select count(*)"));
            pageable = (Pageable) lastArgument;
            querySql += buildLimitSql(pageable);
          } else {
            throw new IllegalArgumentException("Pageable is not the latest argument.");
          }
          List<E> content = findAll(querySql);
          return new Page<E>(pageable.page, pageable.pageSize, count,
            (int) (count / pageable.pageSize), content);
        } else {
          return findOne(querySql);
        }
      } else if (updateQueryPattern.matcher(querySql).matches()
        || deleteQueryPattern.matcher(querySql).matches()) {
        executeQuery(querySql);
        return null;
      }
      throw new MethodUnsupportException(method.toString());
    }
  }

  private Long count(final String countSql) {

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

  private Boolean isSelectQuery(final String querySql) {

    for (Pattern pattern : selectQueryPatterns) {
      if (pattern.matcher(querySql).matches()) {
        return true;
      }
    }
    return false;
  }

  private void executeQuery(final String query) {

    Connection connection = dataSourcePool.getConnection();
    try {
      Statement statement = connection.createStatement();
      System.out.println(query);
      statement.execute(query);
    } catch (SQLException e) {
      throw new MysqlSqlException(e);
    }
    dataSourcePool.freeConnection(connection);
  }

  private List<E> findAll(final String sql) {

    List<E> elements = newArrayList();
    Connection connection = dataSourcePool.getConnection();
    try {
      Statement statement = connection.createStatement();
      System.out.println(sql);
      ResultSet resultSet = statement.executeQuery(sql);
      try {
        while (resultSet.next()) {
          E element = entityClass.newInstance();
          for (Field field : entityFields) {
            if (Modifier.isFinal(field.getModifiers())) {
              continue;
            }
            Object value = getDataFromResultSetByField(resultSet, field);
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

  private E findOne(final String sql) {

    E result = null;
    Connection connection = dataSourcePool.getConnection();
    try {
      Statement statement = connection.createStatement();
      ResultSet resultSet = statement.executeQuery(sql);
      try {
        while (resultSet.next()) {
          result = entityClass.newInstance();
          for (Field field : entityFields) {
            Object value = getDataFromResultSetByField(resultSet, field);
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

  private String buildLimitSql(final Pageable pageable) {

    if (pageable == null) {
      return "";
    } else {
      String orderBySql = null;
      if (pageable.direction != null && ArrayUtils.isNotEmpty(pageable.fields)) {
        StringBuilder orderBySqlBuilder = new StringBuilder(" ORDER BY ");
        for (String fieldName : pageable.fields) {
          Field field = fieldNameToField.get(fieldName);
          String columnName = getFieldColumnName(field);
          orderBySqlBuilder.append(columnName);
          orderBySqlBuilder.append(" ").append(pageable.direction.name()).append(",");
        }
        orderBySql = orderBySqlBuilder.substring(0, orderBySqlBuilder.length() - 1);
      }
      Integer from = pageable.page * pageable.pageSize;
      String limitSql = " limit " + from + "," + pageable.pageSize;
      return orderBySql == null ? limitSql : orderBySql + limitSql;
    }
  }

  private void lockTable(final Statement statement) throws SQLException {

    statement.execute(lockTableSql);
  }

  private void unlockTable(final Statement statement) throws SQLException {

    statement.execute(unlockTableSql);
  }

  private Object getDataFromResultSetByField(final ResultSet resultSet, final Field field)
    throws SQLException {

    Object value = null;
    Type genericType = field.getGenericType();
    String fieldName = getFieldColumnName(field);
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
    return value;
  }

  private String getFieldColumnName(final Field field) {

    Column column = field.getAnnotation(Column.class);
    return column == null ? field.getName() : StringUtils.isBlank(column.name()) ? field.getName()
      : column.name();
  }

  private String coverToSqlValue(final Object value) {

    if (value == null) {
      return "null";
    } else if (value instanceof String) {
      return format("'%s'", value);
    } else if (isCollection(value) || isArray(value)) {
      @SuppressWarnings("rawtypes")
      Collection<?> values = isCollection(value) ? (Collection) value : ArrayUtils.toList(value,
        Object.class);
      List<String> coverdValues = new ArrayList<String>(values.size());
      for (Object o : values) {
        coverdValues.add(coverToSqlValue(o));
      }
      return CollectionUtils.join(coverdValues, ",");
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

  private Map<String, Field> fieldNameToField = newLinkedHashMap();

  private Map<String, Method> methodNameToMethod = newHashMap();

  private DataSourcePool dataSourcePool;

  private Pattern deleteQueryPattern = Pattern.compile("^delete.*", Pattern.CASE_INSENSITIVE);

  private Pattern updateQueryPattern = Pattern.compile("^update.*", Pattern.CASE_INSENSITIVE);

  private Pattern simpleSelectQueryPattern = Pattern.compile("^from.*", Pattern.CASE_INSENSITIVE);

  private Pattern[] selectQueryPatterns = { Pattern.compile("^select.*", Pattern.CASE_INSENSITIVE),
      simpleSelectQueryPattern };

  static {
    Method[] mysqlDaoMethods = MysqlDao.class.getDeclaredMethods();
    for (Method method : mysqlDaoMethods) {
      if (method.getName().equals("count")) {
        countMethod = method;
      }
      if (method.getName().equals("delete")) {
        deleteMethod = method;
      }
      if (method.getName().equals("save")) {
        saveMethod = method;
      }
      if (method.getName().equals("findAll") && ArrayUtils.count(method.getParameterTypes()) == 0) {
        findAllMethod = method;
      }
      if (method.getName().equals("findAll") && ArrayUtils.count(method.getParameterTypes()) == 1
        && method.getParameterTypes()[0].equals(Pageable.class)) {
        pageableFindAllMethod = method;
      }
      if (method.getName().equals("findOne")) {
        findOneMethod = method;
      }
    }
  }

  private static Method findOneMethod;

  private static Method findAllMethod;

  private static Method pageableFindAllMethod;

  private static Method saveMethod;

  private static Method deleteMethod;

  private static Method countMethod;

}
