package com.jeffdisher.laminar.network.p2p;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

import com.jeffdisher.laminar.network.p2p.DownstreamMessage;
import com.jeffdisher.laminar.network.p2p.DownstreamPayload_Identity;
import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.ConfigEntry;


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
}
