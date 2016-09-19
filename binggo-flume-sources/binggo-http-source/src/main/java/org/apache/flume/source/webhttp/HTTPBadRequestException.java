package org.apache.flume.source.webhttp;

import org.apache.flume.FlumeException;

/**
 * Exception thrown by an HTTP Handler if the request was not parsed correctly
 * into an event because the request was not in the expected format.
 * @author Administrator
 *
 */
public class HTTPBadRequestException extends FlumeException {
	  private static final long serialVersionUID = -3540764742069390951L;

	  public HTTPBadRequestException(String msg) {
	    super(msg);
	  }

	  public HTTPBadRequestException(String msg, Throwable th) {
	    super(msg, th);
	  }

	  public HTTPBadRequestException(Throwable th) {
	    super(th);
	  }
}
