package org.apache.flume.source.kafka;

import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;

import org.apache.flume.Context;
import org.apache.flume.conf.ConfigurationException;

public class KafkaSourceUtils {
	private static final Logger log = LoggerFactory.getLogger(KafkaSourceUtils.class);
	
	/**
	 * get properties of kafka
	 * @param context
	 * @return
	 */
	public static Properties getKafkaProperties(Context context) {
		log.info("context={}", context.toString());
		Properties props =  generateDefaultKafkaProps(); // default config
		setKafkaProps(context,props); // get config in config file
		addDocumentedKafkaProps(context,props); // check the important properties
		return props;
	}
	
	public static ConsumerConnector getConsumer(Properties kafkaProps) {
		ConsumerConfig consumerConfig = new ConsumerConfig(kafkaProps);
		ConsumerConnector consumer = Consumer.createJavaConsumerConnector(consumerConfig);
		return consumer;
	}
	/**
	 * Generate consumer properties object with some defaults
	 * @return
	 */
	private static Properties generateDefaultKafkaProps() {
		Properties props = new Properties();
		props.put(KafkaSourceConstants.AUTO_COMMIT_ENABLED, KafkaSourceConstants.DEFAULT_AUTO_COMMIT);
		props.put(KafkaSourceConstants.CONSUMER_TIMEOUT, KafkaSourceConstants.DEFAULT_CONSUMER_TIMEOUT);
		props.put(KafkaSourceConstants.GROUP_ID, KafkaSourceConstants.DEFAULT_GROUP_ID);
		return props;
	}
	
	/**
	 * Add all configuration parameters starting with "kafka" to consumer properties
	 * @param context
	 * @param kafkaProps
	 */
	private static void setKafkaProps(Context context,Properties kafkaProps) {
		Map<String, String> kafkaProperties = context.getSubProperties(KafkaSourceConstants.PROPERTY_PREFIX);
		
		for (Map.Entry<String, String> prop : kafkaProperties.entrySet()) {
			kafkaProps.put(prop.getKey(), prop.getValue());
			
			if (log.isDebugEnabled()) {
				log.debug(String.format("Reading a kafka consumer property: key=%s, value=%s", prop.getKey(), prop.getValue()));
			}
		}	
	}
	
	/**
	 * Some of the consumer properties are especially important, We documented them and gave them a camel-case name to 
	 * match Flume config. If user set these, we will override any existing parameters with these settings.
	 * @param context
	 * @param kafkaProps
	 */
	private static void addDocumentedKafkaProps(Context context, Properties kafkaProps) throws ConfigurationException {
		String zkConnect = context.getString(KafkaSourceConstants.ZOOKEEPER_CONNECT_FLUME);
		if (zkConnect == null) {
			throw new ConfigurationException("ZookeeperConnect must contain at least one ZooKeeper server");
		}
		
		String groupID = context.getString(KafkaSourceConstants.GROUP_ID_FLUME);
		if (groupID == null) {
			kafkaProps.put(KafkaSourceConstants.GROUP_ID, groupID);
		}
	}
	
}
