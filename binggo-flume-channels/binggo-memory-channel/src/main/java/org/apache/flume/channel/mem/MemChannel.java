package org.apache.flume.channel.mem;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flume.annotations.Disposable;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.channel.BasicChannelSemantics;
import org.apache.flume.channel.BasicTransactionSemantics;

@InterfaceAudience.Private
@InterfaceStability.Stable
@Disposable
public class MemChannel extends BasicChannelSemantics
{

	@Override
	protected BasicTransactionSemantics createTransaction() {
		// TODO Auto-generated method stub
		return null;
	}
	
}
