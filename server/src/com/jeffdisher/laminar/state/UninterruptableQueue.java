package com.jeffdisher.laminar.state;

import java.util.concurrent.LinkedBlockingQueue;

import com.jeffdisher.laminar.utils.Assert;


/**
 * Just a wrapper of a LinkedBlockingQueue to avoid the need to handle interrupted exceptions everywhere since our
 * common case (threads we create/own/hide) never uses them.
 * This also allows for a more restricted API to make sure no accidental incorrect uses happen.
 */
public class UninterruptableQueue {
	private final LinkedBlockingQueue<Runnable> _queue = new LinkedBlockingQueue<>();

	/**
	 * Adds the given Runnable to the end of the queue.
	 * 
	 * @param input The Runnable to add to the queue.
	 */
	public void put(Runnable input) {
		try {
			_queue.put(input);
		} catch (InterruptedException e) {
			// Only used in statically uninterruptable cases.
			throw Assert.unexpected(e);
		}
	}

	/**
	 * Block until a Runnable becomes available, removing it from the head of the queue and returning it, when it does.
	 * 
	 * @return The Runnable from the head of the queue.
	 */
	public Runnable blockingGet() {
		try {
			return _queue.take();
		} catch (InterruptedException e) {
			// Only used in statically uninterruptable cases.
			throw Assert.unexpected(e);
		}
	}
}
