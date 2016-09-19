package org.apache.flume.source.webhttp;

import java.util.List;
import javax.servlet.http.HttpServletRequest;

import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;


public interface HTTPSourceHandler extends Configurable {
	
	/**
	  * Takes an {@linkplain HttpServletRequest} and returns a list of Flume
	  * Events. If this request cannot be parsed into Flume events based on the
	  * format this method will throw an exception. This method may also throw an
	  * exception if there is some sort of other error. <p>
	  *
	  * @param request The request to be parsed into Flume events.
	  * @return List of Flume events generated from the request.
	  * @throws HTTPBadRequestException If the was not parsed correctly into an
	  * event because the request was not in the expected format.
	  * @throws Exception If there was an unexpected error.
	  */
	public List<Event> getEvents(HttpServletRequest request) throws HTTPBadRequestException, Exception;

}
