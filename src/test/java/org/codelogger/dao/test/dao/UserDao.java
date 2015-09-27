package org.codelogger.dao.test.dao;

import org.codelogger.dao.MysqlDao;
import org.codelogger.dao.test.domain.User;

public interface UserDao extends MysqlDao<User, Long> {

}
