package org.codelogger.dao;

import java.io.Serializable;
import java.util.List;

import org.codelogger.core.bean.Page;
import org.codelogger.core.bean.Pageable;

public interface MysqlDao<E, I extends Serializable> {

  E findOne(I id);

  List<E> findAll();

  Page<E> findAll(Pageable pageable);

  E save(E e);

  void delete(I id);

  Long count();
}
