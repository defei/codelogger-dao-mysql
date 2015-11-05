package org.codelogger.dao.mysql;

import static com.google.common.collect.Lists.newArrayList;
import static org.codelogger.utils.PrintUtils.println;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Properties;

import org.codelogger.dao.MysqlDaoConstructFactory;
import org.codelogger.dao.test.dao.UserDao;
import org.codelogger.dao.test.domain.User;
import org.junit.Before;
import org.junit.Test;

public class MysqlDaoInterpreterTest {

  private UserDao userDao;

  @Before
  public void setup() throws UnsupportedEncodingException, IOException, InstantiationException,
    IllegalAccessException {

    Properties conf = new Properties();
    conf.load(new InputStreamReader(MysqlDaoInterpreterTest.class
      .getResourceAsStream("/codelogger-dao.config"), "utf-8"));
    userDao = new MysqlDaoConstructFactory(conf).newInstance(UserDao.class);
  }

  @Test
  public void test() {

    println(userDao.count());
    println(userDao.findAll());
    println(userDao.findOne(1L));
    println(userDao.findByEmail("dengdefei@gmail.com"));
    println(userDao.findByName("邓德飞"));
    userDao.delete(5L);
    println(userDao.count());
    User testUser = userDao.findOne(3L);
    testUser.setName("TestUser" + System.currentTimeMillis());
    userDao.save(testUser);
    userDao.delete(4L);
    println(userDao.count());
    println(userDao.findAll());
    User newUser = new User();
    newUser.setName("New User");
    newUser.setEmail(System.currentTimeMillis() + "@codelogger.org");
    newUser = userDao.save(newUser);
    println(newUser);
    println(userDao.count());
    userDao.updateName(4L, "路人甲");
    println(userDao.findOne(4L));
    userDao.deleteUser(4L);
    println(userDao.findAll());
    List<Long> ids = newArrayList(1L, 2L);
    println(userDao.selectUser(ids));

  }
}
