package com.jeffdisher.laminar.network;

import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import com.jeffdisher.laminar.components.NetworkManager;
import com.jeffdisher.laminar.network.p2p.UpstreamResponse;
import com.jeffdisher.laminar.types.ConfigEntry;


public class TestUpstreamPeerManager {
	@Test
	public void testSyncMutationSuccess() throws Throwable {
		UpstreamPeerManager manager = new UpstreamPeerManager();
		ConfigEntry entry = _createEntry();
		TestingToken upstream = new TestingToken();
		
		// Connect them.
		manager.newUpstreamConnected(upstream);
		manager.establishPeer(entry, upstream, 3L);
		UpstreamResponse peerState = manager.commitToSendNextMessage(upstream, false);
		Assert.assertEquals(UpstreamResponse.Type.PEER_STATE, peerState.type);
		manager.setNodeWritable(upstream);
		
		// Receive the mutation.
		manager.didApplyReceivedMutation(upstream, 4L, 3L);
		UpstreamResponse ack = manager.commitToSendNextMessage(upstream, false);
		Assert.assertEquals(UpstreamResponse.Type.RECEIVED_MUTATIONS, ack.type);
	}

	@Test
	public void testSyncMutationReject() throws Throwable {
		UpstreamPeerManager manager = new UpstreamPeerManager();
		ConfigEntry entry = _createEntry();
		TestingToken upstream = new TestingToken();
		
		// Connect them.
		manager.newUpstreamConnected(upstream);
		manager.establishPeer(entry, upstream, 3L);
		UpstreamResponse peerState = manager.commitToSendNextMessage(upstream, false);
		Assert.assertEquals(UpstreamResponse.Type.PEER_STATE, peerState.type);
		manager.setNodeWritable(upstream);
		
		// Reject the mutation.
		manager.failedToApplyMutations(upstream, 3L);
		UpstreamResponse ack = manager.commitToSendNextMessage(upstream, false);
		Assert.assertEquals(UpstreamResponse.Type.PEER_STATE, ack.type);
	}


	private static ConfigEntry _createEntry() {
		return new ConfigEntry(UUID.randomUUID(), null, null);
	}


	private static class TestingToken extends NetworkManager.NodeToken {
	}
}
