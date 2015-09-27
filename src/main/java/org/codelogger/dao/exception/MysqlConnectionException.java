package org.codelogger.dao.exception;

public class MysqlConnectionException extends RuntimeException {

  private static final long serialVersionUID = 2738181071917793387L;

  public MysqlConnectionException() {

    super();
  }

  public MysqlConnectionException(final String message, final Throwable cause,
    final boolean enableSuppression, final boolean writableStackTrace) {

    super(message, cause, enableSuppression, writableStackTrace);
  }

  public MysqlConnectionException(final String message, final Throwable cause) {

    super(message, cause);
  }

  public MysqlConnectionException(final String message) {

    super(message);
  }

  public MysqlConnectionException(final Throwable cause) {

    super(cause);
  }

}
