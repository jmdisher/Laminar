package com.jeffdisher.laminar.network.p2p;

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

import com.jeffdisher.laminar.network.p2p.UpstreamPayload_PeerState;
import com.jeffdisher.laminar.network.p2p.UpstreamResponse;


public class TestUpstreamResponse {
	@Test
	public void testPeerState() throws Throwable {
		long lastReceivedMutationOffset = 10001L;
		UpstreamResponse response = UpstreamResponse.peerState(lastReceivedMutationOffset);
		int size = response.serializedSize();
		ByteBuffer buffer = ByteBuffer.allocate(size);
		response.serializeInto(buffer);
		buffer.flip();
		
		UpstreamResponse test = UpstreamResponse.deserializeFrom(buffer);
		UpstreamPayload_PeerState payload = (UpstreamPayload_PeerState)test.payload;
		Assert.assertEquals(lastReceivedMutationOffset, payload.lastReceivedMutationOffset);
	}

	@Test
	public void testReceivedMutations() throws Throwable {
		long lastReceivedMutationOffset = 10001L;
		UpstreamResponse response = UpstreamResponse.receivedMutations(lastReceivedMutationOffset);
		int size = response.serializedSize();
		ByteBuffer buffer = ByteBuffer.allocate(size);
		response.serializeInto(buffer);
		buffer.flip();
		
		UpstreamResponse test = UpstreamResponse.deserializeFrom(buffer);
		UpstreamPayload_ReceivedMutations payload = (UpstreamPayload_ReceivedMutations)test.payload;
		Assert.assertEquals(lastReceivedMutationOffset, payload.lastReceivedMutationOffset);
	}

	@Test
	public void testCastVote() throws Throwable {
		long termNumber = 2L;
		UpstreamResponse response = UpstreamResponse.castVote(termNumber);
		int size = response.serializedSize();
		ByteBuffer buffer = ByteBuffer.allocate(size);
		response.serializeInto(buffer);
		buffer.flip();
		
		UpstreamResponse test = UpstreamResponse.deserializeFrom(buffer);
		UpstreamPayload_CastVote payload = (UpstreamPayload_CastVote)test.payload;
		Assert.assertEquals(termNumber, payload.termNumber);
	}
}
