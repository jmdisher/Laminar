package com.jeffdisher.laminar.network.p2p;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import com.jeffdisher.laminar.network.p2p.DownstreamMessage;
import com.jeffdisher.laminar.network.p2p.DownstreamPayload_Identity;
import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.MutationRecord;
import com.jeffdisher.laminar.types.MutationRecordType;


public class TestDownstreamMessage {
	@Test
	public void testIdentity() throws Throwable {
		InetAddress localhost = InetAddress.getLocalHost();
		InetSocketAddress cluster = ClusterConfig.cleanSocketAddress(new InetSocketAddress(localhost, 1000));
		InetSocketAddress client = ClusterConfig.cleanSocketAddress(new InetSocketAddress(localhost, 1001));
		ConfigEntry entry = new ConfigEntry(cluster, client);
		DownstreamMessage message = DownstreamMessage.identity(entry);
		int size = message.serializedSize();
		ByteBuffer buffer = ByteBuffer.allocate(size);
		message.serializeInto(buffer);
		buffer.flip();
		
		DownstreamMessage test = DownstreamMessage.deserializeFrom(buffer);
		DownstreamPayload_Identity payload = (DownstreamPayload_Identity)test.payload;
		Assert.assertEquals(entry, payload.self);
	}

	@Test
	public void testAppendMutations() throws Throwable {
		MutationRecord mutation = MutationRecord.generateRecord(MutationRecordType.TEMP, 1L, 1L, UUID.randomUUID(), 1L, new byte[] {1,2,3});
		long lastCommittedMutationOffset = 1L;
		DownstreamMessage message = DownstreamMessage.appendMutations(mutation, lastCommittedMutationOffset);
		int size = message.serializedSize();
		ByteBuffer buffer = ByteBuffer.allocate(size);
		message.serializeInto(buffer);
		buffer.flip();
		
		DownstreamMessage test = DownstreamMessage.deserializeFrom(buffer);
		DownstreamPayload_AppendMutations payload = (DownstreamPayload_AppendMutations)test.payload;
		Assert.assertEquals(lastCommittedMutationOffset, payload.lastCommittedMutationOffset);
		Assert.assertArrayEquals(mutation.payload, payload.records[0].payload);
	}

	@Test
	public void testHeartbeat() throws Throwable {
		long lastCommittedMutationOffset = 1L;
		DownstreamMessage message = DownstreamMessage.heartbeat(lastCommittedMutationOffset);
		int size = message.serializedSize();
		ByteBuffer buffer = ByteBuffer.allocate(size);
		message.serializeInto(buffer);
		buffer.flip();
		
		DownstreamMessage test = DownstreamMessage.deserializeFrom(buffer);
		DownstreamPayload_AppendMutations payload = (DownstreamPayload_AppendMutations)test.payload;
		Assert.assertEquals(lastCommittedMutationOffset, payload.lastCommittedMutationOffset);
		Assert.assertEquals(0, payload.records.length);
	}
}
