package org.apache.flume.sink.kafka;

import java.util.Properties;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Throwables;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.kafka.KafkaSinkCounter;
import org.apache.flume.sink.AbstractSink;


public class KafkaSink extends AbstractSink implements Configurable
{
	private static final Logger log = LoggerFactory.getLogger(KafkaSink.class);
	
	public static final String KEY_HDR = "key";
	public static final String TOPIC_HDR = "topic";
	private Properties kafkaProps;
	private Producer<String, byte[]> producer;
	private String topic;
	private int batchSize;
	private List<KeyedMessage<String, byte[]>> messageList;
	private KafkaSinkCounter counter;
	
	public Status process() throws EventDeliveryException {
		Status result = Status.READY;
	    Channel channel = getChannel();
	    Transaction transaction = null;
	    Event event = null;
	    String eventTopic = null;
	    String eventKey = null;

	    try {
	    	long processedEvents = 0;

	    	transaction = channel.getTransaction();
	    	transaction.begin();

	    	messageList.clear();
	    	for (; processedEvents < batchSize; processedEvents += 1) {
	    		event = channel.take();

	    		if (event == null) {
	    			// no events available in channel
	    			break;
	    		}

	    		byte[] eventBody = event.getBody();
	    		Map<String, String> headers = event.getHeaders();

	    		if ((eventTopic = headers.get(TOPIC_HDR)) == null) {
	    			eventTopic = topic;
	    		}

	    		eventKey = headers.get(KEY_HDR);

	    		if (log.isDebugEnabled()) {
	    			log.debug("{Event} " + eventTopic + " : " + eventKey + " : " + new String(eventBody, "UTF-8"));
	    			log.debug("event #{}", processedEvents);
	    		}

	    		// create a message and add to buffer
	    		KeyedMessage<String, byte[]> data = new KeyedMessage<String, byte[]>(eventTopic, eventKey, eventBody);
	    		messageList.add(data);
	    	}

	    	// publish batch and commit.
	    	if (processedEvents > 0) {
	    		long startTime = System.nanoTime();
	    		producer.send(messageList);
	    		long endTime = System.nanoTime();
	    		counter.addToKafkaEventSendTimer((endTime-startTime)/(1000*1000));
	    		counter.addToEventDrainSuccessCount(Long.valueOf(messageList.size()));
	    	}

	    	transaction.commit();

	    } catch (Exception ex) {
	    	String errorMsg = "Failed to publish events";
	    	log.error("Failed to publish events", ex);
	    	result = Status.BACKOFF;
	    	if (transaction != null) {
	    		try {
	    			transaction.rollback();
	    			counter.incrementRollbackCount();
	    		} catch (Exception e) {
	    			log.error("Transaction rollback failed", e);
	    			throw Throwables.propagate(e);
	    		}
	    	}
	    	throw new EventDeliveryException(errorMsg, ex);
	    } finally {
	    	if (transaction != null) {
	    		transaction.close();
	    	}
	    }

	    return result;
	}
	
	public void configure(Context context) {
		batchSize = context.getInteger(KafkaSinkConstants.BATCH_SIZE, KafkaSinkConstants.DEFAULT_BATCH_SIZE);
		messageList = new ArrayList<KeyedMessage<String, byte[]>>(batchSize);
		log.debug("Using batch size: {}", batchSize);

		topic = context.getString(KafkaSinkConstants.TOPIC, KafkaSinkConstants.DEFAULT_TOPIC);
		if (topic.equals(KafkaSinkConstants.DEFAULT_TOPIC)) {
			log.warn("The Property 'topic' is not set. Using the default topic name: " +
			        KafkaSinkConstants.DEFAULT_TOPIC);
		} else {
			log.info("Using the static topic: " + topic + " this may be over-ridden by event headers");
		}

		kafkaProps = KafkaSinkUtils.getKafkaProperties(context);

		if (log.isDebugEnabled()) {
			log.debug("Kafka producer properties: " + kafkaProps);
		}

		if (counter == null) {
			counter = new KafkaSinkCounter(getName());
		}
	}
	
	@Override
	public synchronized void start() {
		// instantiate the producer
	    ProducerConfig config = new ProducerConfig(kafkaProps);
	    producer = new Producer<String, byte[]>(config);
	    counter.start();
	    super.start();
	}
	
	@Override
	public synchronized void stop() {
	    producer.close();
	    counter.stop();
	    log.info("Kafka Sink {} stopped. Metrics: {}", getName(), counter);
	    super.stop();
	}
	
}
