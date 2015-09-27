package org.codelogger.dao.mysql;

import static org.codelogger.utils.PrintUtils.println;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Properties;

import org.codelogger.dao.test.dao.UserDao;
import org.codelogger.dao.test.domain.User;
import org.junit.Before;
import org.junit.Test;

public class MysqlDaoInterpreterTest {

  private UserDao userDao;

  @Before
  public void setup() throws UnsupportedEncodingException, IOException {

    Properties conf = new Properties();
    conf.load(new InputStreamReader(MysqlDaoInterpreterTest.class
      .getResourceAsStream("/codelogger-dao.config"), "utf-8"));
    final MysqlDaoInterpreter<User, Long> mysqlDaoInterpreter = new MysqlDaoInterpreter<User, Long>(
      conf, UserDao.class);
    userDao = new UserDao() {

      @Override
      public User save(final User e) {

        return mysqlDaoInterpreter.save(e);
      }

      @Override
      public User findOne(final Long id) {

        return mysqlDaoInterpreter.findOne(id);
      }

      @Override
      public List<User> findAll() {

        return mysqlDaoInterpreter.findAll();
      }

      @Override
      public void delete(final Long id) {

        mysqlDaoInterpreter.delete(id);
      }

      @Override
      public void delete(final User e) {

        mysqlDaoInterpreter.delete(e);
      }

      @Override
      public Long count() {

        return mysqlDaoInterpreter.count();
      }
    };
  }

  @Test
  public void test() {

    Long count = userDao.count();
    println(count);
    List<User> users = userDao.findAll();
    println(users);
  }
}
