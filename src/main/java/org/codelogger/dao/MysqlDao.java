package org.codelogger.dao;

import java.io.Serializable;
import java.util.List;

public interface MysqlDao<E, I extends Serializable> {

  E findOne(I id);

  List<E> findAll();

  E save(E e);

  void delete(I id);

  Long count();
}
