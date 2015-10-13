package org.codelogger.dao.stereotype;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Documented
public @interface Query {

  /**
   * Defines the JPA query to be executed when the annotated method is called.
   */
  String value() default "";

  /**
   * Configures whether the given query is a native one. Defaults to
   * {@literal false}.
   */
  boolean nativeQuery() default false;

}
