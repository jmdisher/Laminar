package com.jeffdisher.laminar.utils;

import java.util.concurrent.CountDownLatch;

import org.junit.Test;


/**
 * Tests around UninterruptibleQueue.
 * Note that the internals wait on wall clock time, down in the class library, so we can't synthesize a clock.
 */
public class TestUninterruptibleQueue {
	/**
	 * Tests normal operation of enqueuing consumers and then pulling them back out in the same order.
	 */
	@Test
	public void testInOrder() throws Throwable {
		UninterruptibleQueue<Integer> queue = new UninterruptibleQueue<>();
		queue.put((i) -> org.junit.Assert.assertEquals(0, i.intValue()));
		queue.put((i) -> org.junit.Assert.assertEquals(1, i.intValue()));
		queue.put((i) -> org.junit.Assert.assertEquals(2, i.intValue()));
		queue.blockingGet().accept(0);
		queue.blockingGet().accept(1);
		queue.blockingGet().accept(2);
	}

	/**
	 * Tests that this normal operation works correctly across threads.
	 */
	@Test
	public void testBlockingThread() throws Throwable {
		Throwable[] error = new Throwable[1];
		UninterruptibleQueue<Integer> queue = new UninterruptibleQueue<>();
		Thread thread = new Thread(() -> {
			try {
				queue.blockingGet().accept(0);
				queue.blockingGet().accept(1);
				queue.blockingGet().accept(2);
			} catch (Throwable t) {
				error[0] = t;
			}
		});
		thread.start();
		queue.put((i) -> org.junit.Assert.assertEquals(0, i.intValue()));
		queue.put((i) -> org.junit.Assert.assertEquals(1, i.intValue()));
		queue.put((i) -> org.junit.Assert.assertEquals(2, i.intValue()));
		thread.join();
		org.junit.Assert.assertNull(error[0]);
	}

	/**
	 * Tests that a priority element added after other elements can still appear earlier.
	 */
	@Test
	public void testPriority() throws Throwable {
		Throwable[] error = new Throwable[1];
		CountDownLatch latch = new CountDownLatch(1);
		UninterruptibleQueue<Integer> queue = new UninterruptibleQueue<>();
		Thread thread = new Thread(() -> {
			try {
				queue.blockingGet().accept(0);
				try {
					latch.await();
				} catch (InterruptedException e) {
					org.junit.Assert.fail();
				}
				queue.blockingGet().accept(1);
				queue.blockingGet().accept(2);
			} catch (Throwable t) {
				error[0] = t;
			}
		});
		thread.start();
		queue.put((i) -> org.junit.Assert.assertEquals(0, i.intValue()));
		queue.put((i) -> org.junit.Assert.assertEquals(2, i.intValue()));
		queue.putPriority((i) -> org.junit.Assert.assertEquals(1, i.intValue()), 0L);
		latch.countDown();
		thread.join();
		org.junit.Assert.assertNull(error[0]);
	}

	/**
	 * Tests that a priority element can still be added to an empty queue and will wake up the listening thread.
	 * Note that this test is sensitive to timing and thread scheduling so it may produce false-passes.
	 */
	@Test
	public void testPriorityWhenEmpty() throws Throwable {
		Throwable[] error = new Throwable[1];
		// This test is very sensitive to timing and thread scheduling so it might not always actually test anything.
		// We use this latch just to delay the main thread to make a useful test more likely.
		CountDownLatch latch = new CountDownLatch(1);
		UninterruptibleQueue<Integer> queue = new UninterruptibleQueue<>();
		Thread thread = new Thread(() -> {
			try {
				latch.countDown();
				queue.blockingGet().accept(0);
			} catch (Throwable t) {
				error[0] = t;
			}
		});
		thread.start();
		latch.await();
		queue.putPriority((i) -> org.junit.Assert.assertEquals(0, i.intValue()), 0L);
		thread.join();
		org.junit.Assert.assertNull(error[0]);
	}

	/**
	 * Tests that a priority item will be run normally even if it is discovered after its scheduled time.
	 * Note that this test takes 200ms due to relying on the wall clock.
	 */
	@Test
	public void testPriorityExpired() throws Throwable {
		Throwable[] error = new Throwable[1];
		CountDownLatch latch = new CountDownLatch(1);
		UninterruptibleQueue<Integer> queue = new UninterruptibleQueue<>();
		Thread thread = new Thread(() -> {
			try {
				queue.blockingGet().accept(0);
				try {
					latch.await();
				} catch (InterruptedException e) {
					org.junit.Assert.fail();
				}
				queue.blockingGet().accept(1);
				queue.blockingGet().accept(2);
			} catch (Throwable t) {
				error[0] = t;
			}
		});
		thread.start();
		queue.put((i) -> org.junit.Assert.assertEquals(0, i.intValue()));
		queue.put((i) -> org.junit.Assert.assertEquals(2, i.intValue()));
		queue.putPriority((i) -> org.junit.Assert.assertEquals(1, i.intValue()), 100L);
		Thread.sleep(200L);
		latch.countDown();
		thread.join();
		org.junit.Assert.assertNull(error[0]);
	}

	/**
	 * Tests that we correctly timeout and select the priority element, even when there is nothing in the normal queue.
	 * Note that this test takes 100ms due to relying on the wall clock.
	 */
	@Test
	public void testPriorityOnly() throws Throwable {
		Throwable[] error = new Throwable[1];
		UninterruptibleQueue<Integer> queue = new UninterruptibleQueue<>();
		Thread thread = new Thread(() -> {
			try {
				queue.blockingGet().accept(0);
				queue.blockingGet().accept(1);
				queue.blockingGet().accept(2);
			} catch (Throwable t) {
				error[0] = t;
			}
		});
		thread.start();
		// In theory, this part of the test can fail if it take tens of milliseconds to run these 3 lines.
		// This could be fixed by forcing the caller to provide the time, instead of the delay, but that will complicate
		// our target use-case.
		queue.putPriority((i) -> org.junit.Assert.assertEquals(0, i.intValue()), 20L);
		queue.putPriority((i) -> org.junit.Assert.assertEquals(2, i.intValue()), 100L);
		queue.putPriority((i) -> org.junit.Assert.assertEquals(1, i.intValue()), 50L);
		thread.join();
		org.junit.Assert.assertNull(error[0]);
	}
}
