package com.jeffdisher.laminar.network;

import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.jeffdisher.laminar.types.TopicName;


public class TestListenerManager {
	@Test
	public void testSimpleCase() throws Throwable {
		TopicName topic = TopicName.fromString("test");
		ListenerManager manager = new ListenerManager();
		ListenerState listener = new ListenerState(null, topic, 0L);
		manager.addWritableListener(listener);
		Set<ListenerState> matched = manager.eventBecameAvailable(topic, 1L);
		Assert.assertEquals(1, matched.size());
		matched = manager.eventBecameAvailable(topic, 1L);
		Assert.assertEquals(0, matched.size());
	}

	@Test
	public void testJoinAfterAvailable() throws Throwable {
		TopicName topic = TopicName.fromString("test");
		ListenerManager manager = new ListenerManager();
		Set<ListenerState> matched = manager.eventBecameAvailable(topic, 1L);
		Assert.assertEquals(0, matched.size());
		matched = manager.eventBecameAvailable(topic, 2L);
		Assert.assertEquals(0, matched.size());
		ListenerState listener = new ListenerState(null, topic, 0L);
		long shouldFetch = manager.addWritableListener(listener);
		Assert.assertEquals(1L, shouldFetch);
		matched = manager.eventBecameAvailable(topic, 1L);
		Assert.assertEquals(1, matched.size());
		matched = manager.eventBecameAvailable(topic, 3L);
		Assert.assertEquals(0, matched.size());
	}

	@Test
	public void testNewEventDoesNotRewindKnownOffsets() throws Throwable {
		TopicName topic = TopicName.fromString("test");
		ListenerManager manager = new ListenerManager();
		ListenerState listener = new ListenerState(null, topic, 0L);
		Set<ListenerState> matched = manager.eventBecameAvailable(topic, 1L);
		Assert.assertEquals(0, matched.size());
		matched = manager.eventBecameAvailable(topic, 2L);
		Assert.assertEquals(0, matched.size());
		matched = manager.eventBecameAvailable(topic, 3L);
		Assert.assertEquals(0, matched.size());
		long shouldFetch = manager.addWritableListener(listener);
		Assert.assertEquals(1L, shouldFetch);
		matched = manager.eventBecameAvailable(topic, 1L);
		Assert.assertEquals(1, matched.size());
		listener.lastSentLocalOffset = 1L;
		shouldFetch = manager.addWritableListener(listener);
		Assert.assertEquals(2L, shouldFetch);
	}
}
