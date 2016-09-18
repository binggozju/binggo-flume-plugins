package org.apache.flume.channel.mem;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import org.apache.flume.ChannelException;
import org.apache.flume.ChannelFullException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.annotations.Disposable;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.channel.BasicChannelSemantics;
import org.apache.flume.channel.BasicTransactionSemantics;
import org.apache.flume.instrumentation.ChannelCounter;

@InterfaceAudience.Private
@InterfaceStability.Stable
@Disposable
public class MemChannel extends BasicChannelSemantics {
	
	private static Logger log = LoggerFactory.getLogger(MemChannel.class);
	
	/**
	 * lock to guard queue, mainly needed to keep it locked down during resizes
	 * it should never be held through a blocking operation
	 */
	private Object queueLock = new Object();
	
	@GuardedBy(value = "queueLock")
	private LinkedBlockingDeque<Event> queue;
	
	/**
	 * invariant that tracks the amount of space remaining in the queue(with all uncommitted takeLists deducted)
	 * we maintain the remaining permits = queue.remaining - takeList.size()
	 * this allows local threads waiting for space in the queue to commit without denying access to the
	 * shared lock to threads that would make more space on the queue
	 */
	private Semaphore queueRemaining;
	
	/**
	 * used to make "reservations" to grab data from the queue.
	 * by using this we can block for a while to get data without locking all other threads out
	 * like we would if we tried to use a blocking call on queue.
	 */
	private Semaphore queueStored;
	
	// maximum items in a transaction queue
	private volatile Integer transCapacity;
	
	private volatile int keepAlive;
	private volatile int byteCapacity;
	private volatile int lastByteCapacity;
	private volatile int byteCapacityBufferPercentage;
	private Semaphore bytesRemaining;
	private ChannelCounter channelCounter;
	
	public MemChannel() {
		super();
	}
	
	@Override
	public void configure(Context context) {
	    Integer capacity = null;
	    try {
	    	capacity = context.getInteger("capacity", MemChannelConstants.defaultCapacity);
	    } catch (NumberFormatException e) {
	    	capacity = MemChannelConstants.defaultCapacity;
	    	log.warn("Invalid capacity specified, initializing channel to default capacity of {}", MemChannelConstants.defaultCapacity);
	    }

	    if (capacity <= 0) {
	    	capacity = MemChannelConstants.defaultCapacity;
	    	log.warn("Invalid capacity specified, initializing channel to default capacity of {}", MemChannelConstants.defaultCapacity);
	    }
	    try {
	    	transCapacity = context.getInteger("transactionCapacity", MemChannelConstants.defaultTransCapacity);
	    } catch (NumberFormatException e) {
	    	transCapacity = MemChannelConstants.defaultTransCapacity;
	    	log.warn("Invalid transation capacity specified, initializing channel to default capacity of {}", MemChannelConstants.defaultTransCapacity);
	    }

	    if (transCapacity <= 0) {
	    	transCapacity = MemChannelConstants.defaultTransCapacity;
	    	log.warn("Invalid transation capacity specified, initializing channel to default capacity of {}", MemChannelConstants.defaultTransCapacity);
	    }
	    Preconditions.checkState(transCapacity <= capacity,
	        "Transaction Capacity of Memory Channel cannot be higher than the capacity.");

	    try {
	    	byteCapacityBufferPercentage = context.getInteger("byteCapacityBufferPercentage", MemChannelConstants.defaultByteCapacityBufferPercentage);
	    } catch (NumberFormatException e) {
	    	byteCapacityBufferPercentage = MemChannelConstants.defaultByteCapacityBufferPercentage;
	    }

	    try {
	    	byteCapacity = (int)((context.getLong("byteCapacity", MemChannelConstants.defaultByteCapacity).longValue() * (1 - byteCapacityBufferPercentage * .01 )) /MemChannelConstants.byteCapacitySlotSize);
	    	if (byteCapacity < 1) {
	    		byteCapacity = Integer.MAX_VALUE;
	    	}
	    } catch (NumberFormatException e) {
	    	byteCapacity = (int)((MemChannelConstants.defaultByteCapacity * (1 - byteCapacityBufferPercentage * .01 )) /MemChannelConstants.byteCapacitySlotSize);
	    }

	    try {
	    	keepAlive = context.getInteger("keep-alive", MemChannelConstants.defaultKeepAlive);
	    } catch (NumberFormatException e) {
	    	keepAlive = MemChannelConstants.defaultKeepAlive;
	    }

	    if(queue != null) {
	    	try {
	    		resizeQueue(capacity);
	    	} catch (InterruptedException e) {
	    	  Thread.currentThread().interrupt();
	    	}
	    } else {
	    	synchronized(queueLock) {
	    		queue = new LinkedBlockingDeque<Event>(capacity);
	    		queueRemaining = new Semaphore(capacity);
	    		queueStored = new Semaphore(0);
	    	}
	    }

	    if (bytesRemaining == null) {
	    	bytesRemaining = new Semaphore(byteCapacity);
	    	lastByteCapacity = byteCapacity;
	    } else {
	    	if (byteCapacity > lastByteCapacity) {
	    		bytesRemaining.release(byteCapacity - lastByteCapacity);
	    		lastByteCapacity = byteCapacity;
	      } else {
	    	  try {
	    		  if(!bytesRemaining.tryAcquire(lastByteCapacity - byteCapacity, keepAlive, TimeUnit.SECONDS)) {
	    			  log.warn("Couldn't acquire permits to downsize the byte capacity, resizing has been aborted");
	    		  } else {
	    			  lastByteCapacity = byteCapacity;
	    		  }
	    	  } catch (InterruptedException e) {
	    		  Thread.currentThread().interrupt();
	    	  }
	      	}
	    }

	    if (channelCounter == null) {
	    	channelCounter = new ChannelCounter(getName());
	    }
	}
	
	@Override
	public synchronized void start() {
		channelCounter.start();
	    channelCounter.setChannelSize(queue.size());
	    channelCounter.setChannelCapacity(Long.valueOf(queue.size() + queue.remainingCapacity()));
	    super.start();
	}
	
	@Override
	public synchronized void stop() {
		channelCounter.setChannelSize(queue.size());
	    channelCounter.stop();
	    super.stop();
	}
		
	@Override
	protected BasicTransactionSemantics createTransaction() {
		return new MemTransaction(transCapacity, channelCounter);
	}
	
	private void resizeQueue(int capacity) throws InterruptedException {
		int oldCapacity;
	    synchronized(queueLock) {
	    	oldCapacity = queue.size() + queue.remainingCapacity();
	    }

	    if(oldCapacity == capacity) {
	    	return;
	    } else if (oldCapacity > capacity) {
	    	if(!queueRemaining.tryAcquire(oldCapacity - capacity, keepAlive, TimeUnit.SECONDS)) {
	    		log.warn("Couldn't acquire permits to downsize the queue, resizing has been aborted");
	    	} else {
	    		synchronized(queueLock) {
	    			LinkedBlockingDeque<Event> newQueue = new LinkedBlockingDeque<Event>(capacity);
	    			newQueue.addAll(queue);
	    			queue = newQueue;
	    		}
	    	}
	    } else {
	    	synchronized(queueLock) {
	    		LinkedBlockingDeque<Event> newQueue = new LinkedBlockingDeque<Event>(capacity);
	    		newQueue.addAll(queue);
	    		queue = newQueue;
	    	}
	    	queueRemaining.release(capacity - oldCapacity);
	    }
	}
	
	private long estimateEventSize(Event event) {
		byte[] body = event.getBody();
	    if(body != null && body.length != 0) {
	    	return body.length;
	    }
	    //Each event occupies at least 1 slot, so return 1.
	    return 1;
	}
	
	/**
	 * MemTransaction
	 * @author Administrator
	 */
	private class MemTransaction extends BasicTransactionSemantics {
		private LinkedBlockingDeque<Event> takeList;
	    private LinkedBlockingDeque<Event> putList;
	    private final ChannelCounter channelCounter;
	    private int putByteCounter = 0;
	    private int takeByteCounter = 0;
	    
	    public MemTransaction(int transCapacity, ChannelCounter counter) {
	        putList = new LinkedBlockingDeque<Event>(transCapacity);
	        takeList = new LinkedBlockingDeque<Event>(transCapacity);

	        channelCounter = counter;
	    }
	    
		@Override
		protected void doPut(Event event) throws InterruptedException {
			channelCounter.incrementEventPutAttemptCount();
			int eventByteSize = (int)Math.ceil(estimateEventSize(event)/MemChannelConstants.byteCapacitySlotSize);

			if (!putList.offer(event)) {
				throw new ChannelException("Put queue for MemTransaction of capacity " +
					putList.size() + " full, consider committing more frequently, " +
		            "increasing capacity or increasing thread count");
			}
			putByteCounter += eventByteSize;	
		}

		@Override
		protected Event doTake() throws InterruptedException {
			channelCounter.incrementEventTakeAttemptCount();
			if(takeList.remainingCapacity() == 0) {
				throw new ChannelException("Take list for MemTransaction, capacity " +
						takeList.size() + " full, consider committing more frequently, " +
						"increasing capacity, or increasing thread count");
			}
			if(!queueStored.tryAcquire(keepAlive, TimeUnit.SECONDS)) {
				return null;
			}
			Event event;
			synchronized(queueLock) {
		        event = queue.poll();
			}
			Preconditions.checkNotNull(event, "Queue.poll returned NULL despite semaphore signalling existence of entry");
			takeList.put(event);

			int eventByteSize = (int)Math.ceil(estimateEventSize(event)/MemChannelConstants.byteCapacitySlotSize);
			takeByteCounter += eventByteSize;
			
			return event;
		}

		@Override
		protected void doCommit() throws InterruptedException {
			int remainingChange = takeList.size() - putList.size();
			if(remainingChange < 0) {
				if(!bytesRemaining.tryAcquire(putByteCounter, keepAlive, TimeUnit.SECONDS)) {
					throw new ChannelException("Cannot commit transaction. Byte capacity " +
		            "allocated to store event body " + byteCapacity * MemChannelConstants.byteCapacitySlotSize +
		            "reached. Please increase heap space/byte capacity allocated to " +
		            "the channel as the sinks may not be keeping up with the sources");
		        }
		        if(!queueRemaining.tryAcquire(-remainingChange, keepAlive, TimeUnit.SECONDS)) {
		          bytesRemaining.release(putByteCounter);
		          throw new ChannelFullException("Space for commit to queue couldn't be acquired." +
		              " Sinks are likely not keeping up with sources, or the buffer size is too tight");
		        }
			}
			
			int puts = putList.size();
			int takes = takeList.size();
			synchronized(queueLock) {
				if(puts > 0 ) {
					while(!putList.isEmpty()) {
		            if(!queue.offer(putList.removeFirst())) {
		              throw new RuntimeException("Queue add failed, this shouldn't be able to happen");
		            }
		          }
		        }
				putList.clear();
		        takeList.clear();
			}

			bytesRemaining.release(takeByteCounter);
			takeByteCounter = 0;
			putByteCounter = 0;

			queueStored.release(puts);
			if(remainingChange > 0) {
		        queueRemaining.release(remainingChange);
			}
			if (puts > 0) {
		        channelCounter.addToEventPutSuccessCount(puts);
			}
			if (takes > 0) {
		        channelCounter.addToEventTakeSuccessCount(takes);
			}

			channelCounter.setChannelSize(queue.size());
		}

		@Override
		protected void doRollback() throws InterruptedException {
			int takes = takeList.size();
			synchronized(queueLock) {
		        Preconditions.checkState(queue.remainingCapacity() >= takeList.size(), "Not enough space in memory channel " +
		            "queue to rollback takes. This should never happen, please report");
		        while(!takeList.isEmpty()) {
		          queue.addFirst(takeList.removeLast());
		        }
		        putList.clear();
			}
			
			bytesRemaining.release(putByteCounter);
			putByteCounter = 0;
			takeByteCounter = 0;

			queueStored.release(takes);
			channelCounter.setChannelSize(queue.size());
		}	
	}
	
}
