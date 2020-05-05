package com.jeffdisher.laminar.state;

import java.util.concurrent.CountDownLatch;

import org.junit.Assert;


/**
 * The standard "Future" interface is too big and has too many error cases we don't need for these tests so this is used
 * by tests, instead.
 * 
 * @param <T> The type returned by the future.
 */
public class F<T> {
	private CountDownLatch _latch = new CountDownLatch(1);
	private T _result;

	public void put(T result) {
		_result = result;
		_latch.countDown();
	}

	public T get() {
		try {
			_latch.await();
		} catch (InterruptedException e) {
			Assert.fail("Not used in tests");
		}
		return _result;
	}
}
