package com.jeffdisher.laminar.state;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

import com.jeffdisher.laminar.utils.Assert;


/**
 * Just a wrapper of a LinkedBlockingQueue to avoid the need to handle interrupted exceptions everywhere since our
 * common case (threads we create/own/hide) never uses them.
 * This also allows for a more restricted API to make sure no accidental incorrect uses happen.
 */
public class UninterruptableQueue {
	private final LinkedBlockingQueue<Consumer<StateSnapshot>> _queue = new LinkedBlockingQueue<>();

	/**
	 * Adds the given StateSnapshot Consumer to the end of the queue.
	 * 
	 * @param input The Consumer to add to the queue.
	 */
	public void put(Consumer<StateSnapshot> input) {
		try {
			_queue.put(input);
		} catch (InterruptedException e) {
			// Only used in statically uninterruptable cases.
			throw Assert.unexpected(e);
		}
	}

	/**
	 * Block until a Consumer becomes available, removing it from the head of the queue and returning it, when it does.
	 * 
	 * @return The Consumer from the head of the queue.
	 */
	public Consumer<StateSnapshot> blockingGet() {
		try {
			return _queue.take();
		} catch (InterruptedException e) {
			// Only used in statically uninterruptable cases.
			throw Assert.unexpected(e);
		}
	}
}
