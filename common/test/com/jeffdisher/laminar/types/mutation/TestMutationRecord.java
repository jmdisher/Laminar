package com.jeffdisher.laminar.types.mutation;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.TopicName;


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
		byte[] payload = new byte[] { 1, 2, 3 };
		MutationRecord record = MutationRecord.temp(termNumber, globalOffset, topic, clientId, clientNonce, payload);
		byte[] serialized = record.serialize();
		MutationRecord deserialized = MutationRecord.deserialize(serialized);
		Assert.assertEquals(record.type, deserialized.type);
		Assert.assertEquals(record.termNumber, deserialized.termNumber);
		Assert.assertEquals(record.globalOffset, deserialized.globalOffset);
		Assert.assertEquals(record.clientId, deserialized.clientId);
		Assert.assertEquals(record.clientNonce, deserialized.clientNonce);
		Assert.assertArrayEquals(((MutationRecordPayload_Temp)record.payload).contents, ((MutationRecordPayload_Temp)deserialized.payload).contents);
	}

	@Test
	public void testInto() throws Throwable {
		long termNumber = 1L;
		long globalOffset = 1L;
		TopicName topic = TopicName.fromString("test");
		UUID clientId = UUID.randomUUID();
		long clientNonce = 1L;
		byte[] payload = new byte[] { 1, 2, 3 };
		MutationRecord record = MutationRecord.temp(termNumber, globalOffset, topic, clientId, clientNonce, payload);
		ByteBuffer buffer = ByteBuffer.allocate(record.serializedSize());
		record.serializeInto(buffer);
		buffer.flip();
		MutationRecord deserialized = MutationRecord.deserializeFrom(buffer);
		Assert.assertEquals(record.type, deserialized.type);
		Assert.assertEquals(record.termNumber, deserialized.termNumber);
		Assert.assertEquals(record.globalOffset, deserialized.globalOffset);
		Assert.assertEquals(record.clientId, deserialized.clientId);
		Assert.assertEquals(record.clientNonce, deserialized.clientNonce);
		Assert.assertArrayEquals(((MutationRecordPayload_Temp)record.payload).contents, ((MutationRecordPayload_Temp)deserialized.payload).contents);
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
		Assert.assertEquals(((MutationRecordPayload_Config)record.payload).config.entries.length, ((MutationRecordPayload_Config)deserialized.payload).config.entries.length);
	}
}
