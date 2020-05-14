package com.jeffdisher.laminar.types;

import java.nio.ByteBuffer;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;


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
		MutationRecord record = MutationRecord.generateRecord(MutationRecordType.TEMP, termNumber, globalOffset, topic, clientId, clientNonce, payload);
		byte[] serialized = record.serialize();
		MutationRecord deserialized = MutationRecord.deserialize(serialized);
		Assert.assertEquals(record.type, deserialized.type);
		Assert.assertEquals(record.termNumber, deserialized.termNumber);
		Assert.assertEquals(record.globalOffset, deserialized.globalOffset);
		Assert.assertEquals(record.clientId, deserialized.clientId);
		Assert.assertEquals(record.clientNonce, deserialized.clientNonce);
		Assert.assertArrayEquals(record.payload, deserialized.payload);
	}

	@Test
	public void testInto() throws Throwable {
		long termNumber = 1L;
		long globalOffset = 1L;
		TopicName topic = TopicName.fromString("test");
		UUID clientId = UUID.randomUUID();
		long clientNonce = 1L;
		byte[] payload = new byte[] { 1, 2, 3 };
		MutationRecord record = MutationRecord.generateRecord(MutationRecordType.TEMP, termNumber, globalOffset, topic, clientId, clientNonce, payload);
		ByteBuffer buffer = ByteBuffer.allocate(record.serializedSize());
		record.serializeInto(buffer);
		buffer.flip();
		MutationRecord deserialized = MutationRecord.deserializeFrom(buffer);
		Assert.assertEquals(record.type, deserialized.type);
		Assert.assertEquals(record.termNumber, deserialized.termNumber);
		Assert.assertEquals(record.globalOffset, deserialized.globalOffset);
		Assert.assertEquals(record.clientId, deserialized.clientId);
		Assert.assertEquals(record.clientNonce, deserialized.clientNonce);
		Assert.assertArrayEquals(record.payload, deserialized.payload);
	}
}
