package org.codelogger.dao.exception;

public class MethodUnsupportException extends RuntimeException {

  private static final long serialVersionUID = 6374041140342379867L;

  public MethodUnsupportException() {

    super();
  }

  public MethodUnsupportException(final String message, final Throwable cause,
    final boolean enableSuppression, final boolean writableStackTrace) {

    super(message, cause, enableSuppression, writableStackTrace);
  }

  public MethodUnsupportException(final String message, final Throwable cause) {

    super(message, cause);
  }

  public MethodUnsupportException(final String message) {

    super(message);
  }

  public MethodUnsupportException(final Throwable cause) {

    super(cause);
  }

}
