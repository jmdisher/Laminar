package com.jeffdisher.laminar.utils;

import java.util.PriorityQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;


/**
 * Just a wrapper of a LinkedBlockingQueue to avoid the need to handle interrupted exceptions everywhere since our
 * common case (threads we create/own/hide) never uses them.
 * This also allows for a more restricted API to make sure no accidental incorrect uses happen.
 * Additionally, it is possible to priority schedule a Consumer to become available after a delay.
 * 
 * @param <T> The type passed to the consumer.
 */
public class UninterruptibleQueue<T> {
	private final LinkedBlockingQueue<ConsumerWrapper<T>> _queue = new LinkedBlockingQueue<>();
	private final PriorityQueue<DelayedConsumer<T>> _delayed = new PriorityQueue<>();
	private final Object _priorityLock = new Object();

	/**
	 * Adds the given Consumer to the end of the queue.
	 * 
	 * @param input The Consumer to add to the queue.
	 */
	public void put(Consumer<T> input) {
		if (null == input) {
			throw new NullPointerException("Cannot enqueue null consumer");
		}
		try {
			_queue.put(new ConsumerWrapper<T>(input));
		} catch (InterruptedException e) {
			// Only used in statically uninterruptible cases.
			throw Assert.unexpected(e);
		}
	}

	/**
	 * Schedules a Consumer to be returned after a certain number of milliseconds from now.
	 * Note that these scheduled Consumers are considered higher priority than normal ones, once the waiting time has
	 * passed, and will be returned even before a normal Consumer which was added first.
	 * 
	 * @param input The Consumer to add to the queue.
	 * @param delayMillis The number of milliseconds from now when this Consumer becomes valid.
	 */
	public void putPriority(Consumer<T> input, long delayMillis) {
		if (null == input) {
			throw new NullPointerException("Cannot enqueue null consumer");
		}
		long now = System.currentTimeMillis();
		long validTime = now + delayMillis;
		if (validTime < 0) {
			// We consider it invalid to have a negative time (although that could happen in the far future).
			throw new IllegalArgumentException("Scheduled time must be positive");
		}
		synchronized (_priorityLock) {
			_delayed.add(new DelayedConsumer<T>(input, validTime));
		}
		// Drop a sentinel in here to wake up the queue.
		try {
			_queue.put(new ConsumerWrapper<T>(null));
		} catch (InterruptedException e) {
			// Only used in statically uninterruptible cases.
			throw Assert.unexpected(e);
		}
	}

	/**
	 * Block until a Consumer becomes available, removing it from the head of the queue and returning it, when it does.
	 * Note that a priority-scheduled element will be preferentially returned, so long as it is after its wait time,
	 * even though it may have been added earlier than the alternative in the normal queue.
	 * 
	 * @return The Consumer from the head of the queue.
	 */
	public Consumer<T> blockingGet() {
		try {
			Consumer<T> result = null;
			// Note that we loop on result because our sentinel value and timeout scenarios are handled more elegantly, that way.
			while (null == result) {
				// Check if we have a priority element.
				long delay = 0;
				synchronized (_priorityLock) {
					DelayedConsumer<T> priority = _delayed.peek();
					if (null != priority) {
						// Priority element so only wait until it is ready.
						long now = System.currentTimeMillis();
						delay = priority.scheduledTimeMillis - now;
						if (delay <= 0L) {
							// This is already ready so just return it.
							result = _delayed.remove().consumer;
						}
					}
				}
				if (null == result) {
					if (delay > 0L) {
						ConsumerWrapper<T> wrapper = _queue.poll(delay, TimeUnit.MILLISECONDS);
						if (null != wrapper) {
							result = wrapper.consumer;
						}
					} else {
						// Block until there is something.
						result = _queue.take().consumer;
					}
				}
			}
			return result;
		} catch (InterruptedException e) {
			// Only used in statically uninterruptible cases.
			throw Assert.unexpected(e);
		}
	}


	/**
	 * We use a wrapper for the Consumer in the queue since we have no safe way to interrupt a blocking take/poll
	 * without a great deal of additional complexity.  Using a wrapper, we can drop a sentinel value into the queue
	 * whenever we put something in the priority queue, thus allowing the take/poll to return.
	 * We can then detect the sentinel value and use that to retry, now observing the priority queue.
	 */
	private static class ConsumerWrapper<T> {
		public Consumer<T> consumer;
		public ConsumerWrapper(Consumer<T> consumer) {
			this.consumer = consumer;
		}
	}


	/**
	 * We just put our priority consumers into this wrapper so we can sort them by their scheduled time.
	 */
	private static class DelayedConsumer<T> implements Comparable<DelayedConsumer<T>> {
		public Consumer<T> consumer;
		public long scheduledTimeMillis;
		public DelayedConsumer(Consumer<T> consumer, long scheduledTimeMillis) {
			this.consumer = consumer;
			this.scheduledTimeMillis= scheduledTimeMillis;
		}
		@Override
		public int compareTo(DelayedConsumer<T> arg0) {
			// We will collapse this to +/- 1, just to avoid potential overflow (although unlikely for this usage).
			return Long.signum(this.scheduledTimeMillis - arg0.scheduledTimeMillis);
		}
	}
}
