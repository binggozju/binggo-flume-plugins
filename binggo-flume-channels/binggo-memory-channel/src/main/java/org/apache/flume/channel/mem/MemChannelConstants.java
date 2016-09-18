package org.apache.flume.channel.mem;

public class MemChannelConstants {
	
	  public static final Integer defaultCapacity = 100;
	  public static final Integer defaultTransCapacity = 100;
	  public static final double byteCapacitySlotSize = 100;
	  public static final Long defaultByteCapacity = (long)(Runtime.getRuntime().maxMemory() * .80);
	  public static final Integer defaultByteCapacityBufferPercentage = 20;

	  public static final Integer defaultKeepAlive = 3;

}
