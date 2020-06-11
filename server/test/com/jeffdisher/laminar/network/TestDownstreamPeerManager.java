package com.jeffdisher.laminar.network;

import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import com.jeffdisher.laminar.components.NetworkManager;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.Intention;
import com.jeffdisher.laminar.types.TopicName;


public class TestDownstreamPeerManager {
	/**
	 * Tests common behaviour during normal operation of a LEADER.
	 */
	@Test
	public void testCommonSyncFlow() throws Throwable {
		DownstreamPeerManager manager = new DownstreamPeerManager();
		ConfigEntry self = _createEntry();
		ConfigEntry entry1 = _createEntry();
		TestingToken token1 = new TestingToken();
		ConfigEntry entry2 = _createEntry();
		TestingToken token2 = new TestingToken();
		manager.createNewPeer(entry1, token1);
		manager.createNewPeer(entry2, token2);
		
		// Should be 0 connected peers - then connect them and there should be 2.
		Assert.assertEquals(0, manager.immutablePeersConnected().size());
		ReadOnlyDownstreamPeerState peer1 = manager.nodeDidConnect(token1);
		Assert.assertFalse(peer1.isReadyForSend());
		ReadOnlyDownstreamPeerState peer2 = manager.nodeDidConnect(token2);
		Assert.assertFalse(peer2.isReadyForSend());
		Assert.assertEquals(2, manager.immutablePeersConnected().size());
		
		// We then send identity and handle the peer state.
		peer1.commitToSendIdentity(self, 1L);
		peer2.commitToSendIdentity(self, 2L);
		manager.nodeDidHandshake(token1, 3L);
		manager.nodeDidHandshake(token2, 4L);
		
		// Set one writable and see that we get one.
		manager.setNodeWritable(token1);
		Assert.assertEquals(1, manager.immutablePeersReadyToReceiveIntention(4L).size());
		Assert.assertEquals(0, manager.immutablePeersReadyToReceiveIntention(5L).size());
		manager.setNodeWritable(token2);
		Assert.assertEquals(1, manager.immutablePeersReadyToReceiveIntention(5L).size());
		
		// Use one of these and see that it is no longer here.
		TopicName topic = TopicName.fromString("test");
		Intention mutation = Intention.put(1L, 4L, topic, UUID.randomUUID(), 1, new byte[0], new byte[0]);
		manager.immutablePeersReadyToReceiveIntention(4L).iterator().next().commitToSendIntentions(1L, 1L, mutation, 1L, 1L);
		Assert.assertEquals(0, manager.immutablePeersReadyToReceiveIntention(4L).size());
		
		// Check the case where it acks and both nodes are now at the same point.
		manager.nodeDidAckIntention(token1, mutation.intentionOffset);
		manager.setNodeWritable(token1);
		Assert.assertEquals(2, manager.immutablePeersReadyToReceiveIntention(5L).size());
	}


	private static ConfigEntry _createEntry() {
		return new ConfigEntry(UUID.randomUUID(), null, null);
	}


	private static class TestingToken extends NetworkManager.NodeToken {
	}
}
