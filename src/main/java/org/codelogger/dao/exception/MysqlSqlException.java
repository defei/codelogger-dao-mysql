package org.codelogger.dao.exception;

public class MysqlSqlException extends RuntimeException {

  private static final long serialVersionUID = -6888688534815273086L;

  public MysqlSqlException() {

    super();
  }

  public MysqlSqlException(final String message, final Throwable cause,
    final boolean enableSuppression, final boolean writableStackTrace) {

    super(message, cause, enableSuppression, writableStackTrace);
  }

  public MysqlSqlException(final String message, final Throwable cause) {

    super(message, cause);
  }

  public MysqlSqlException(final String message) {

    super(message);
  }

  public MysqlSqlException(final Throwable cause) {

    super(cause);
  }

}
