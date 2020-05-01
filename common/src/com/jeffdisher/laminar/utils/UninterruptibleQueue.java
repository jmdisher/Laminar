package com.jeffdisher.laminar.utils;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;


/**
 * Just a wrapper of a LinkedBlockingQueue to avoid the need to handle interrupted exceptions everywhere since our
 * common case (threads we create/own/hide) never uses them.
 * This also allows for a more restricted API to make sure no accidental incorrect uses happen.
 * 
 * @param <T> The type passed to the consumer.
 */
public class UninterruptibleQueue<T> {
	private final LinkedBlockingQueue<Consumer<T>> _queue = new LinkedBlockingQueue<>();

	/**
	 * Adds the given Consumer to the end of the queue.
	 * 
	 * @param input The Consumer to add to the queue.
	 */
	public void put(Consumer<T> input) {
		try {
			_queue.put(input);
		} catch (InterruptedException e) {
			// Only used in statically uninterruptible cases.
			throw Assert.unexpected(e);
		}
	}

	/**
	 * Block until a Consumer becomes available, removing it from the head of the queue and returning it, when it does.
	 * 
	 * @return The Consumer from the head of the queue.
	 */
	public Consumer<T> blockingGet() {
		try {
			return _queue.take();
		} catch (InterruptedException e) {
			// Only used in statically uninterruptible cases.
			throw Assert.unexpected(e);
		}
	}
}
