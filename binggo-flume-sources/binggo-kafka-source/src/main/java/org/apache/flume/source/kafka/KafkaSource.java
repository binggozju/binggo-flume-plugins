package org.apache.flume.source.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.kafka.KafkaSourceCounter;
import org.apache.flume.source.AbstractSource;

public class KafkaSource extends AbstractSource implements Configurable, PollableSource {

	private static final Logger log = LoggerFactory.getLogger(KafkaSource.class);
	private ConsumerConnector consumer;	// consumer of kafka
	private ConsumerIterator<byte[],byte[]> it;
	private String topic;
	
	private int batchUpperLimit;
	private int timeUpperLimit;
	private int consumerTimeout;
	private boolean kafkaAutoCommitEnabled;
	
	private Context context;		// configuration info in flume's configuration file
	private Properties kafkaProps;	// properties relative with kafka consumer
	
	private final List<Event> eventList = new ArrayList<Event>();
	private KafkaSourceCounter counter;	
	
	// process a batch of received events
	public Status process() throws EventDeliveryException {
		byte[] kafkaMessage;
		byte[] kafkaKey;
		Event event;
		Map<String, String> headers;
		long batchStartTime = System.currentTimeMillis();
		long batchEndTime = System.currentTimeMillis() + timeUpperLimit;
		try {
			boolean iterStatus = false;
			long startTime = System.nanoTime();
			while (eventList.size() < batchUpperLimit && System.currentTimeMillis() < batchEndTime) {
				iterStatus = hasNext();
				if (iterStatus) {
					// get next message
					MessageAndMetadata<byte[], byte[]> messageAndMetadata = it.next();
					kafkaKey = messageAndMetadata.key();
					kafkaMessage = messageAndMetadata.message();
					
					// Add headers to event (topic, timestamp, and key)
					headers = new HashMap<String, String>();
					headers.put(KafkaSourceConstants.TIMESTAMP, String.valueOf(System.currentTimeMillis()));
					headers.put(KafkaSourceConstants.TOPIC, topic);
					
					if (kafkaKey != null) {
						headers.put(KafkaSourceConstants.KEY, new String(kafkaKey));
					}
					
					if (log.isDebugEnabled()) {
						log.debug("Message: {}", new String(kafkaMessage));
					}
					event = EventBuilder.withBody(kafkaMessage, headers);
					eventList.add(event);	
				}
				
		        if (log.isDebugEnabled()) {
		        	log.debug("Waited: {} ", System.currentTimeMillis() - batchStartTime);
		            log.debug("Event #: {}", eventList.size());
		         }
			}
			
			long endTime = System.nanoTime();
			counter.addToKafkaEventGetTimer((endTime-startTime)/(1000*1000));
			counter.addToEventReceivedCount(Long.valueOf(eventList.size()));
			
			// If we have events, send events to channel, clear the event list,
			// and commit if Kafka doesn't auto-commit
		    if (eventList.size() > 0) {
		    	getChannelProcessor().processEventBatch(eventList);
		    	counter.addToEventAcceptedCount(eventList.size());
		    	eventList.clear();
		    	
		    	if (log.isDebugEnabled()) {
		    		log.debug("Wrote {} events to channel", eventList.size());
		    	}
		    	if (!kafkaAutoCommitEnabled) {
		            // commit the read transactions to Kafka to avoid duplicates
		            long commitStartTime = System.nanoTime();
		            consumer.commitOffsets();
		            
		            long commitEndTime = System.nanoTime();
		            counter.addToKafkaCommitTimer((commitEndTime-commitStartTime)/(1000*1000));
		        }
		    }
		    
		    if (!iterStatus) {
		        if (log.isDebugEnabled()) {
		        	counter.incrementKafkaEmptyCount();
		            log.debug("Returning with backoff. No more data to read");
		        }
		        return Status.BACKOFF;
		    }
		    
		    return Status.READY;
	
		} catch (Exception ex) {
			log.error("KafkaSource Exception, {}", ex);
			return Status.BACKOFF;
		}
	}

	public void configure(Context context) {
		this.context = context;
	    batchUpperLimit = context.getInteger(KafkaSourceConstants.BATCH_SIZE,
	            KafkaSourceConstants.DEFAULT_BATCH_SIZE);
	    timeUpperLimit = context.getInteger(KafkaSourceConstants.BATCH_DURATION_MS,
	            KafkaSourceConstants.DEFAULT_BATCH_DURATION);
	    topic = context.getString(KafkaSourceConstants.TOPIC);
	    
	    if (topic == null) {
	    	throw new ConfigurationException("Kafka topic must be specified.");
	    }
		
	    kafkaProps = KafkaSourceUtils.getKafkaProperties(context);
	    consumerTimeout = Integer.parseInt(kafkaProps.getProperty(KafkaSourceConstants.CONSUMER_TIMEOUT));
	    kafkaAutoCommitEnabled = Boolean.parseBoolean(kafkaProps.getProperty(KafkaSourceConstants.AUTO_COMMIT_ENABLED));
	    
	    if (counter == null) {
	    	counter = new KafkaSourceCounter(getName());
	    }  
	}
	
	@Override
	public synchronized void start() {
		log.info("Starting {}", this);
		try {
			consumer = KafkaSourceUtils.getConsumer(kafkaProps);
		} catch (Exception ex) {
			throw new FlumeException("Unable to create consumer.", ex);
		}
		
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		// one thread per topic
		topicCountMap.put(topic, 1);
		
		// Get the message iterator for our topic
		// currently we only support a single topic
		try {
			Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
			List<KafkaStream<byte[], byte[]>> topicList = consumerMap.get(topic);
			KafkaStream<byte[], byte[]> stream = topicList.get(0);
			it = stream.iterator();
		} catch (Exception ex) {
			throw new FlumeException("Unable to get message iterator from Kafka", ex);
		}
		
		log.info("Kafka source {} started.", getName());
		counter.start();
		super.start();	
	}
	
	@Override
	public synchronized void stop() {
		if (consumer != null) {
			// syncs offsets of messages read to ZooKeeper to avoid reading the same messages again
			consumer.shutdown();
		}
		
		counter.stop();
		log.info("Kafka Source {} stopped. Metrics: {}", getName(), counter);
		super.stop();
	}
	
	boolean hasNext() {
		try {
			it.hasNext();
			return true;
		} catch (ConsumerTimeoutException ex) {
			return false;
		}
	}

}
