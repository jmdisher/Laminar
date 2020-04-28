package com.jeffdisher.laminar.types;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

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
		
		ClusterConfig config = ClusterConfig.configFromEntries(new ConfigEntry[] {new ConfigEntry(new InetSocketAddress(address, clusterPort), new InetSocketAddress(address, clientPort))});
		byte[] serialized = config.serialize();
		ClusterConfig deserialized = ClusterConfig.deserialize(serialized);
		Assert.assertArrayEquals(config.entries, deserialized.entries);
	}

	@Test
	public void testLocalHostByteBuffer() throws Throwable {
		InetAddress address = InetAddress.getLocalHost();
		int clusterPort = 2000;
		int clientPort = 2001;
		
		ClusterConfig config = ClusterConfig.configFromEntries(new ConfigEntry[] {new ConfigEntry(new InetSocketAddress(address, clusterPort), new InetSocketAddress(address, clientPort))});
		ByteBuffer buffer = ByteBuffer.allocate(config.serializedSize());
		config.serializeInto(buffer);
		buffer.flip();
		ClusterConfig deserialized = ClusterConfig.deserializeFrom(buffer);
		Assert.assertArrayEquals(config.entries, deserialized.entries);
	}

	@Test
	public void testEquality() throws Throwable {
		InetAddress address = InetAddress.getLocalHost();
		int clusterPort = 2000;
		int clientPort = 2001;
		int clientPort2 = 2002;
		ConfigEntry entry1 = new ConfigEntry(new InetSocketAddress(address, clusterPort), new InetSocketAddress(address, clientPort));
		ConfigEntry entry2 = new ConfigEntry(new InetSocketAddress(address, clusterPort), new InetSocketAddress(address, clientPort));
		ConfigEntry diff1 = new ConfigEntry(new InetSocketAddress(address, clusterPort), new InetSocketAddress(address, clientPort2));
		Assert.assertEquals(entry1.hashCode(), entry2.hashCode());
		// We know how the hash works so we know that changing the port will change it.
		Assert.assertNotEquals(entry1.hashCode(), diff1.hashCode());
		Assert.assertEquals(entry1, entry2);
		Assert.assertNotEquals(entry1, diff1);
	}
}
