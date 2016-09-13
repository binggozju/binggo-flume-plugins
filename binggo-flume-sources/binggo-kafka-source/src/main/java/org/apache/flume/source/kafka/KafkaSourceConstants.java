package org.apache.flume.source.kafka;

public class KafkaSourceConstants {
	  public static final String TOPIC = "topic";
	  public static final String KEY = "key";
	  public static final String TIMESTAMP = "timestamp";
	  public static final String BATCH_SIZE = "batchSize";
	  public static final String BATCH_DURATION_MS = "batchDurationMillis";
	  public static final String CONSUMER_TIMEOUT = "consumer.timeout.ms";
	  public static final String AUTO_COMMIT_ENABLED = "auto.commit.enable";
	  public static final String ZOOKEEPER_CONNECT = "zookeeper.connect";
	  public static final String ZOOKEEPER_CONNECT_FLUME = "zookeeperConnect";
	  public static final String GROUP_ID = "group.id";
	  public static final String GROUP_ID_FLUME = "groupId";
	  public static final String PROPERTY_PREFIX = "kafka.";


	  public static final int DEFAULT_BATCH_SIZE = 1000;
	  public static final int DEFAULT_BATCH_DURATION = 1000;
	  public static final String DEFAULT_CONSUMER_TIMEOUT = "10";
	  public static final String DEFAULT_AUTO_COMMIT =  "false";
	  public static final String DEFAULT_GROUP_ID = "flume";

}
