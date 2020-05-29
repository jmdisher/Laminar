package com.jeffdisher.laminar.types.mutation;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.types.payload.Payload_Config;
import com.jeffdisher.laminar.types.payload.Payload_Put;


/**
 * Tests around serialization and deserialization of MutationRecord objects.
 */
public class TestMutationRecord {
	@Test
	public void testBasic() throws Throwable {
		long termNumber = 1L;
		long globalOffset = 1L;
		TopicName topic = TopicName.fromString("test");
		UUID clientId = UUID.randomUUID();
		long clientNonce = 1L;
		byte[] key = "key".getBytes(StandardCharsets.UTF_8);
		byte[] value = "value".getBytes(StandardCharsets.UTF_8);
		MutationRecord record = MutationRecord.put(termNumber, globalOffset, topic, clientId, clientNonce, key, value);
		byte[] serialized = record.serialize();
		MutationRecord deserialized = MutationRecord.deserialize(serialized);
		Assert.assertEquals(record.type, deserialized.type);
		Assert.assertEquals(record.termNumber, deserialized.termNumber);
		Assert.assertEquals(record.globalOffset, deserialized.globalOffset);
		Assert.assertEquals(record.clientId, deserialized.clientId);
		Assert.assertEquals(record.clientNonce, deserialized.clientNonce);
		Assert.assertArrayEquals(((Payload_Put)record.payload).key, ((Payload_Put)deserialized.payload).key);
		Assert.assertArrayEquals(((Payload_Put)record.payload).value, ((Payload_Put)deserialized.payload).value);
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
		MutationRecord record = MutationRecord.put(termNumber, globalOffset, topic, clientId, clientNonce, key, value);
		ByteBuffer buffer = ByteBuffer.allocate(record.serializedSize());
		record.serializeInto(buffer);
		buffer.flip();
		MutationRecord deserialized = MutationRecord.deserializeFrom(buffer);
		Assert.assertEquals(record.type, deserialized.type);
		Assert.assertEquals(record.termNumber, deserialized.termNumber);
		Assert.assertEquals(record.globalOffset, deserialized.globalOffset);
		Assert.assertEquals(record.clientId, deserialized.clientId);
		Assert.assertEquals(record.clientNonce, deserialized.clientNonce);
		Assert.assertArrayEquals(((Payload_Put)record.payload).key, ((Payload_Put)deserialized.payload).key);
		Assert.assertArrayEquals(((Payload_Put)record.payload).value, ((Payload_Put)deserialized.payload).value);
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
		MutationRecord record = MutationRecord.updateConfig(termNumber, globalOffset, clientId, clientNonce, config);
		ByteBuffer buffer = ByteBuffer.allocate(record.serializedSize());
		record.serializeInto(buffer);
		buffer.flip();
		MutationRecord deserialized = MutationRecord.deserializeFrom(buffer);
		Assert.assertEquals(record.type, deserialized.type);
		Assert.assertEquals(record.termNumber, deserialized.termNumber);
		Assert.assertEquals(record.globalOffset, deserialized.globalOffset);
		Assert.assertEquals(record.clientId, deserialized.clientId);
		Assert.assertEquals(record.clientNonce, deserialized.clientNonce);
		Assert.assertEquals(((Payload_Config)record.payload).config.entries.length, ((Payload_Config)deserialized.payload).config.entries.length);
	}
}
