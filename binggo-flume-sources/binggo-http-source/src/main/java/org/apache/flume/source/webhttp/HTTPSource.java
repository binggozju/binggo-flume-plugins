package org.apache.flume.source.webhttp;

import javax.net.ssl.SSLServerSocket;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.*;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.security.SslSocketConnector;
import org.mortbay.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.tools.HTTPServerConstraintUtil;


public class HTTPSource extends AbstractSource implements EventDrivenSource, Configurable {
	/*
	 * There are 2 ways of doing this:
	 * a. Have a static server instance and use connectors in each source
	 *    which binds to the port defined for that source.
	 * b. Each source starts its own server instance, which binds to the source's
	 *    port.
	 *
	 * b is more efficient than a because Jetty does not allow binding a
	 * servlet to a connector. So each request will need to go through each
	 * each of the handlers/servlet till the correct one is found.
	 *
	 */
	private static final Logger logger = LoggerFactory.getLogger(HTTPSource.class);
	private volatile Integer port;
	private volatile Server srv;
	private volatile String host;
	private HTTPSourceHandler handler;
	private SourceCounter sourceCounter;

	// SSL configuration variable
	private volatile String keyStorePath;
	private volatile String keyStorePassword;
	private volatile Boolean sslEnabled;
	private final List<String> excludedProtocols = new LinkedList<String>();
	
	
	public void configure(Context context) {
		try {
			// SSL related config
	        sslEnabled = context.getBoolean(HTTPSourceConstants.SSL_ENABLED, false);

	        port = context.getInteger(HTTPSourceConstants.CONFIG_PORT);
	        host = context.getString(HTTPSourceConstants.CONFIG_BIND, HTTPSourceConstants.DEFAULT_BIND);

	        Preconditions.checkState(host != null && !host.isEmpty(), "HTTPSource hostname specified is empty");
	        Preconditions.checkNotNull(port, "HTTPSource requires a port number to be specified");

	        String handlerClassName = context.getString(HTTPSourceConstants.CONFIG_HANDLER,
	                HTTPSourceConstants.DEFAULT_HANDLER).trim();

	        if(sslEnabled) {
	        	logger.debug("SSL configuration enabled");
	        	keyStorePath = context.getString(HTTPSourceConstants.SSL_KEYSTORE);
	        	Preconditions.checkArgument(keyStorePath != null && !keyStorePath.isEmpty(),
	        			"Keystore is required for SSL Conifguration" );
	        	keyStorePassword = context.getString(HTTPSourceConstants.SSL_KEYSTORE_PASSWORD);
	        	Preconditions.checkArgument(keyStorePassword != null,
	        			"Keystore password is required for SSL Configuration");
	        	String excludeProtocolsStr = context.getString(HTTPSourceConstants.EXCLUDE_PROTOCOLS);
	        	if (excludeProtocolsStr == null) {
	        		excludedProtocols.add("SSLv3");
	        	} else {
	        		excludedProtocols.addAll(Arrays.asList(excludeProtocolsStr.split(" ")));
	        		if (!excludedProtocols.contains("SSLv3")) {
	        			excludedProtocols.add("SSLv3");
	        		}
	        	}
	        }

	        @SuppressWarnings("unchecked")
			Class<? extends HTTPSourceHandler> clazz = (Class<? extends HTTPSourceHandler>)Class.forName(handlerClassName);
	        handler = clazz.getDeclaredConstructor().newInstance();
	        //ref: http://docs.codehaus.org/display/JETTY/Embedding+Jetty
	        //ref: http://jetty.codehaus.org/jetty/jetty-6/apidocs/org/mortbay/jetty/servlet/Context.html
	        Map<String, String> subProps = context.getSubProperties(HTTPSourceConstants.CONFIG_HANDLER_PREFIX);
	        handler.configure(new Context(subProps));
		} catch (ClassNotFoundException ex) {
	        logger.error("Error while configuring HTTPSource. Exception follows.", ex);
	        Throwables.propagate(ex);
		} catch (ClassCastException ex) {
	        logger.error("Deserializer is not an instance of HTTPSourceHandler. Deserializer must implement HTTPSourceHandler.");
	        Throwables.propagate(ex);
		} catch (Exception ex) {
	        logger.error("Error configuring HTTPSource!", ex);
	        Throwables.propagate(ex);
		}
		
		if (sourceCounter == null) {
	        sourceCounter = new SourceCounter(getName());
		}
	}

	@Override
	public void start() {
	    Preconditions.checkState(srv == null, "Running HTTP Server found in source: " + getName() 
	    	+ " before I started one. Will not attempt to start.");
	    srv = new Server();

	    // Connector Array
	    Connector[] connectors = new Connector[1];
	    
	    if (sslEnabled) {
	    	SslSocketConnector sslSocketConnector = new HTTPSourceSocketConnector(excludedProtocols);
	    	sslSocketConnector.setKeystore(keyStorePath);
	    	sslSocketConnector.setKeyPassword(keyStorePassword);
	    	sslSocketConnector.setReuseAddress(true);
	    	connectors[0] = sslSocketConnector;
	    } else {
	    	SelectChannelConnector connector = new SelectChannelConnector();
	    	connector.setReuseAddress(true);
	    	connectors[0] = connector;
	    }

	    connectors[0].setHost(host);
	    connectors[0].setPort(port);
	    srv.setConnectors(connectors);
	    try {
	    	org.mortbay.jetty.servlet.Context root = new org.mortbay.jetty.servlet.Context(srv, "/", 
	    			org.mortbay.jetty.servlet.Context.SESSIONS);
	    	root.addServlet(new ServletHolder(new FlumeHTTPServlet()), "/");
	    	HTTPServerConstraintUtil.enforceConstraints(root);
	    	srv.start();
	    	Preconditions.checkArgument(srv.getHandler().equals(root));
	    } catch (Exception ex) {
	    	logger.error("Error while starting HTTPSource. Exception follows.", ex);
	    	Throwables.propagate(ex);
	    }
	    Preconditions.checkArgument(srv.isRunning());
	    sourceCounter.start();
	    super.start();
	}
	
	@Override
	public void stop() {
		try {
			srv.stop();
			srv.join();
			srv = null;
		} catch (Exception ex) {
			logger.error("Error while stopping HTTPSource. Exception follows.", ex);
		}
		sourceCounter.stop();
		logger.info("Http source {} stopped. Metrics: {}", getName(), sourceCounter);
	}
	
	
	private void checkHostAndPort() {
		Preconditions.checkState(host != null && !host.isEmpty(), "HTTPSource hostname specified is empty");
		Preconditions.checkNotNull(port, "HTTPSource requires a port number to be specified");
	}
	
	/**
	 * 
	 * @author Administrator
	 */
	private class FlumeHTTPServlet extends HttpServlet {
		private static final long serialVersionUID = 4891924863218790344L;

	    @Override
	    public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
	    	List<Event> events = Collections.emptyList(); //create empty list
	    	try {
	    		events = handler.getEvents(request);
	    	} catch (HTTPBadRequestException ex) {
	    		logger.warn("Received bad request from client. ", ex);
	    		response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Bad request from client. " + ex.getMessage());
	    		return;
	    	} catch (Exception ex) {
	    		logger.warn("Deserializer threw unexpected exception. ", ex);
	    		response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Deserializer threw unexpected exception. " + ex.getMessage());
	    		return;
	    	}
	    	sourceCounter.incrementAppendBatchReceivedCount();
	    	sourceCounter.addToEventReceivedCount(events.size());
	    	try {
	    		getChannelProcessor().processEventBatch(events);
	    	} catch (ChannelException ex) {
	    		logger.warn("Error appending event to channel. Channel might be full. Consider increasing the channel "
	                + "capacity or make sure the sinks perform faster.", ex);
	    		response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE,
	                "Error appending event to channel. Channel might be full." + ex.getMessage());
	    		return;
	    	} catch (Exception ex) {
	    		logger.warn("Unexpected error appending event to channel. ", ex);
	    		response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
	                "Unexpected error while appending event to channel. " + ex.getMessage());
	    		return;
	    	}
	    	response.setCharacterEncoding(request.getCharacterEncoding());
	    	response.setStatus(HttpServletResponse.SC_OK);
	    	response.flushBuffer();
	    	sourceCounter.incrementAppendBatchAcceptedCount();
	    	sourceCounter.addToEventAcceptedCount(events.size());
	    }

	    @Override
	    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
	    	doPost(request, response);
	    }
	}
	
	/**
	 * @author Administrator
	 *
	 */
	private static class HTTPSourceSocketConnector extends SslSocketConnector {
		private final List<String> excludedProtocols;
	    HTTPSourceSocketConnector(List<String> excludedProtocols) {
	    	this.excludedProtocols = excludedProtocols;
	    }

	    @Override
	    public ServerSocket newServerSocket(String host, int port, int backlog) throws IOException {
	    	SSLServerSocket socket = (SSLServerSocket)super.newServerSocket(host, port, backlog);
	    	String[] protocols = socket.getEnabledProtocols();
	    	List<String> newProtocols = new ArrayList<String>(protocols.length);
	    	for(String protocol: protocols) {
	    		if (!excludedProtocols.contains(protocol)) {
	    			newProtocols.add(protocol);
	    		}
	    	}
	    	socket.setEnabledProtocols(newProtocols.toArray(new String[newProtocols.size()]));
	    	return socket;
	    }
	}

}
