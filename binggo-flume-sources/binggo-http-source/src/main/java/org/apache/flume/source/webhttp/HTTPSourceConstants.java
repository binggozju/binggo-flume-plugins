package org.apache.flume.source.webhttp;

public class HTTPSourceConstants {
	public static final String CONFIG_PORT = "port";
	public static final String CONFIG_HANDLER = "handler";
	public static final String CONFIG_HANDLER_PREFIX = CONFIG_HANDLER + ".";
	public static final String CONFIG_BIND = "bind";

	public static final String DEFAULT_BIND = "0.0.0.0";

	public static final String DEFAULT_HANDLER = "org.apache.flume.source.webhttp.JSONHandler";

	public static final String SSL_KEYSTORE = "keystore";
	public static final String SSL_KEYSTORE_PASSWORD = "keystorePassword";
	public static final String SSL_ENABLED = "enableSSL";
	public static final String EXCLUDE_PROTOCOLS = "excludeProtocols";
}
