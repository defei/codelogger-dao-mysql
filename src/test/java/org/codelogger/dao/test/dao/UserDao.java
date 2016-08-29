package org.codelogger.dao.test.dao;

import java.util.Collection;
import java.util.List;

import org.codelogger.core.bean.Page;
import org.codelogger.core.bean.Pageable;
import org.codelogger.dao.MysqlDao;
import org.codelogger.dao.stereotype.Param;
import org.codelogger.dao.stereotype.Query;
import org.codelogger.dao.test.domain.User;

public interface UserDao extends MysqlDao<User, Long> {

  public User findByEmail(String name);

  public List<User> findByName(String name);

  public Page<User> findByName(String name, Pageable pageable);

  @Query("from USER where name = :name")
  public Page<User> findByQuery(@Param("name") String name, Pageable pageable);

  @Query("update USER set name = :name where id = :id")
  public void updateName(@Param("id") Long id, @Param("name") String name);

  @Query("delete from USER where id = :id")
  public void deleteUser(@Param("id") Long id);

  @Query("from USER where id in (:ids)")
  public List<User> selectUser(@Param("ids") Collection<Long> ids);

}
