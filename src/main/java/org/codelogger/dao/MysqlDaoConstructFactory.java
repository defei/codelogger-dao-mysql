package org.codelogger.dao;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Properties;

import org.codelogger.core.context.bean.DefaultConstructFactory;
import org.codelogger.dao.mysql.MysqlDaoInterpreter;

public class MysqlDaoConstructFactory extends DefaultConstructFactory {

  public MysqlDaoConstructFactory(final Properties configurations) {

    super(configurations);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T newInstance(final Class<T> componentClass) throws InstantiationException,
    IllegalAccessException {

    @SuppressWarnings("rawtypes")
    final MysqlDaoInterpreter mysqlDaoInterpreter = new MysqlDaoInterpreter(configurations,
      componentClass);
    InvocationHandler invocationHandler = new InvocationHandler() {

      @Override
      public Object invoke(final Object proxy, final Method method, final Object[] args)
        throws Throwable {

        return mysqlDaoInterpreter.executeByMethod(method, args);
      }

    };
    return (T) Proxy.newProxyInstance(this.getClass().getClassLoader(),
      new Class[] { componentClass }, invocationHandler);
  }

}
