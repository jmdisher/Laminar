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
	private volatile boolean _didCall;

	public void put(T result) {
		_result = result;
		_didCall = true;
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

	/**
	 * Used when a test wants to verify that something was NOT called by a specific point in test.  Note that this call
	 * doesn't synchronize with anything so it is important that the caller use other means to ensure that the result
	 * won't change in racy ways.
	 * 
	 * @return Whether or not the result has been populated.
	 */
	public boolean pollDidCall() {
		return _didCall;
	}
}
