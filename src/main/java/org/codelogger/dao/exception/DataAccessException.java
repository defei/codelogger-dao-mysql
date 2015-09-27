package org.codelogger.dao.exception;

public class DataAccessException extends RuntimeException {

  private static final long serialVersionUID = 6374041140342379867L;

  public DataAccessException() {

    super();
  }

  public DataAccessException(final String message, final Throwable cause,
    final boolean enableSuppression, final boolean writableStackTrace) {

    super(message, cause, enableSuppression, writableStackTrace);
  }

  public DataAccessException(final String message, final Throwable cause) {

    super(message, cause);
  }

  public DataAccessException(final String message) {

    super(message);
  }

  public DataAccessException(final Throwable cause) {

    super(cause);
  }

}
