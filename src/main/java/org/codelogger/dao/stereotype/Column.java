package org.codelogger.dao.stereotype;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Column {

  /**
   * (Optional) The name of the column. Defaults to the property or field name.
   */
  String name() default "";

  /**
   * (Optional) The column length. (Applies only if a string-valued column is
   * used.)
   */
  int length() default 255;

  /**
   * (Optional) Whether the database column is nullable.
   */
  boolean nullable() default true;

  /**
   * (Optional) Whether the column is a unique key. This is a shortcut for the
   * <code>UniqueConstraint</code> annotation at the table level and is useful
   * for when the unique key constraint corresponds to only a single column.
   * This constraint applies in addition to any constraint entailed by primary
   * key mapping and to constraints specified at the table level.
   */
  boolean unique() default false;

}
