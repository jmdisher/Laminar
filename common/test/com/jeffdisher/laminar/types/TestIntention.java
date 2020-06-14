package com.jeffdisher.laminar.types;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.Intention;
import com.jeffdisher.laminar.types.TopicName;


/**
 * Tests around serialization and deserialization of Intention objects.
 */
public class TestIntention {
	@Test
	public void testBasic() throws Throwable {
		long termNumber = 1L;
		long globalOffset = 1L;
		TopicName topic = TopicName.fromString("test");
		UUID clientId = UUID.randomUUID();
		long clientNonce = 1L;
		byte[] key = "key".getBytes(StandardCharsets.UTF_8);
		byte[] value = "value".getBytes(StandardCharsets.UTF_8);
		Intention record = Intention.put(termNumber, globalOffset, topic, clientId, clientNonce, key, value);
		byte[] serialized = record.serialize();
		Intention deserialized = Intention.deserialize(serialized);
		Assert.assertEquals(record, deserialized);
	}

	@Test
	public void testInto() throws Throwable {
		long termNumber = 1L;
		long globalOffset = 1L;
		TopicName topic = TopicName.fromString("test");
		UUID clientId = UUID.randomUUID();
		long clientNonce = 1L;
		byte[] key = "key".getBytes(StandardCharsets.UTF_8);
		byte[] value = "value".getBytes(StandardCharsets.UTF_8);
		Intention record = Intention.put(termNumber, globalOffset, topic, clientId, clientNonce, key, value);
		ByteBuffer buffer = ByteBuffer.allocate(record.serializedSize());
		record.serializeInto(buffer);
		buffer.flip();
		Intention deserialized = Intention.deserializeFrom(buffer);
		Assert.assertEquals(record, deserialized);
	}

	@Test
	public void testConfig() throws Throwable {
		ClusterConfig config = ClusterConfig.configFromEntries(new ConfigEntry[] {
				new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(11), new InetSocketAddress(21)),
				new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(12), new InetSocketAddress(22)),
		});
		long termNumber = 1L;
		long globalOffset = 1L;
		UUID clientId = UUID.randomUUID();
		long clientNonce = 1L;
		Intention record = Intention.updateConfig(termNumber, globalOffset, clientId, clientNonce, config);
		ByteBuffer buffer = ByteBuffer.allocate(record.serializedSize());
		record.serializeInto(buffer);
		buffer.flip();
		Intention deserialized = Intention.deserializeFrom(buffer);
		Assert.assertEquals(record, deserialized);
	}
}
