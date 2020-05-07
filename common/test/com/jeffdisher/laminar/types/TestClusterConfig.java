package com.jeffdisher.laminar.types;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;


/**
 * Tests around serialization and deserialization of ClusterConfig objects.
 */
public class TestClusterConfig {
	@Test
	public void testLocalHost() throws Throwable {
		InetAddress address = InetAddress.getLocalHost();
		int clusterPort = 2000;
		int clientPort = 2001;
		
		ClusterConfig config = ClusterConfig.configFromEntries(new ConfigEntry[] {new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(address, clusterPort), new InetSocketAddress(address, clientPort))});
		byte[] serialized = config.serialize();
		ClusterConfig deserialized = ClusterConfig.deserialize(serialized);
		Assert.assertEquals(config.entries.length, deserialized.entries.length);
		Assert.assertEquals(config.entries[0].nodeUuid, deserialized.entries[0].nodeUuid);
		Assert.assertEquals(config.entries[0].cluster, deserialized.entries[0].cluster);
		Assert.assertEquals(config.entries[0].client, deserialized.entries[0].client);
	}

	@Test
	public void testLocalHostByteBuffer() throws Throwable {
		InetAddress address = InetAddress.getLocalHost();
		int clusterPort = 2000;
		int clientPort = 2001;
		
		ClusterConfig config = ClusterConfig.configFromEntries(new ConfigEntry[] {new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(address, clusterPort), new InetSocketAddress(address, clientPort))});
		ByteBuffer buffer = ByteBuffer.allocate(config.serializedSize());
		config.serializeInto(buffer);
		buffer.flip();
		ClusterConfig deserialized = ClusterConfig.deserializeFrom(buffer);
		Assert.assertEquals(config.entries.length, deserialized.entries.length);
		Assert.assertEquals(config.entries[0].nodeUuid, deserialized.entries[0].nodeUuid);
		Assert.assertEquals(config.entries[0].cluster, deserialized.entries[0].cluster);
		Assert.assertEquals(config.entries[0].client, deserialized.entries[0].client);
	}
}
